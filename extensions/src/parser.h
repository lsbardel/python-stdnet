/*
 * Copyright (c) 2013, Luca Sbardella <luca dot sbardella at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __READIS_PARSER_H__
#define __READIS_PARSER_H__

#include <Python.h>
#include <memory>
#include <cstdlib>
#include <string>
#include <sstream>
#include <deque>

#define CRLF "\r\n"
#define RESPONSE_INTEGER  ':'
#define RESPONSE_STRING  '$'
#define RESPONSE_ARRAY  '*'
#define RESPONSE_STATUS  '+'
#define RESPONSE_ERROR  '-'


class Task;
class StringTask;
class ArrayTask;
typedef std::shared_ptr<Task> TaskPtr;
typedef std::string string;
typedef long long integer;

class RedisParser {
public:
    RedisParser(PyObject *protocolError, PyObject *replyError):
        protocolError(protocolError), replyError(replyError) {}

    ~RedisParser(){}
    //
    void feed(const char* data);
    PyObject* get();
    PyObject* get_buffer() const;
private:
    std::deque<TaskPtr> stack;
    PyObject *protocolError;
    PyObject *replyError;
    string buffer;
    //
    PyObject* task(TaskPtr& task);
    PyObject* _get_new();
    RedisParser();
    //
    friend class Task;
    friend class StringTask;
    friend class ArrayTask;
};


class Task {
public:
    Task(integer length):length(length){}
    virtual ~Task() {}
    virtual PyObject* decode(RedisParser& parser) = 0;
protected:
    integer length;
private:
    Task();
    Task(const Task&);
};

class StringTask: public Task {
public:
    StringTask(integer length):Task(length) {}
    PyObject* decode(RedisParser& parser);
private:
    string str;
};


class ArrayTask: public Task {
public:
    ArrayTask(integer length):Task(length), array(PyList_New(0)) {}
    PyObject* decode(RedisParser& parser);
private:
    PyObject *array;
};

//
// Obatin a python string from a c++ stringstream buffer
inline PyObject* pybytes(const string& value) {
    return Py_BuildValue("y#", value.c_str(), value.size());
}
//
inline PyObject* pybytes_tuple(const string& value) {
    return Py_BuildValue("(y#)", value.c_str(), value.size());
}
//
inline PyObject* pylong(const string& value) {
    long long resp = atoi(value.c_str());
    return PyLong_FromLongLong(resp);
}

//
// read data from the buffer and put it into string str. If the data is ready
// return true, otherwise false.
inline bool read_buffer(string& buffer, string& str) {
    integer size = buffer.find(CRLF);
    if (size >= 0) {
        str.append(buffer.substr(0, size));
        buffer.erase(0, size+2);
        return true;
    } else {
        return false;
    }
}
//
inline bool read_buffer(string& buffer, string& str, integer size) {
    if (buffer.size() >= size+2) {
        str.append(buffer.substr(0, size));
        buffer.erase(0, size+2);
        return true;
    } else {
        return false;
    }
}

inline void RedisParser::feed(const char* data) {
    this->buffer.append(data);
}

inline PyObject* RedisParser::get() {
    PyObject* result;
    if (this->stack.size()) {
        TaskPtr task(this->stack.front());
        this->stack.pop_front();
        result = task->decode(*this);
        if (!result) {
            this->stack.push_front(task);
        }
    } else {
        result = this->_get_new();
    }
    return result ? result : Py_BuildValue("");
}

inline PyObject* RedisParser::get_buffer() const {
    return pybytes(this->buffer);
}

inline PyObject* RedisParser::_get_new() {
    string response;
    if (read_buffer(this->buffer, response)) {
        char rtype(response.at(0));
        response.erase(0,1);
        switch(rtype) {
        case RESPONSE_STATUS:
            return pybytes(response);
        case RESPONSE_INTEGER:
            return pylong(response);
        case RESPONSE_ERROR: {
            PyObject* args = pybytes_tuple(response);
            return PyObject_CallObject(this->replyError, args);
        }
        case RESPONSE_STRING: {
            TaskPtr task(new StringTask(atoi(response.c_str())));
            return this->task(task);
        }
        case RESPONSE_ARRAY: {
            TaskPtr atask(new ArrayTask(atoi(response.c_str())));
            return this->task(atask);
        }
        default:
            return PyObject_CallObject(this->protocolError, NULL);
        }
    } else {
        return NULL;
    }
}

inline PyObject* RedisParser::task(TaskPtr& task) {
    PyObject* result = task->decode(*this);
    if (!result) {
        this->stack.push_back(task);
    }
    return result;
}

inline PyObject* StringTask::decode(RedisParser& parser) {
    string str;
    if(read_buffer(parser.buffer, str, this->length)) {
        return pybytes(str);
    } else {
        return NULL;
    }
}

inline PyObject* ArrayTask::decode(RedisParser& parser) {
    PyObject* response;
    while (this->length > 0) {
        response = parser._get_new();
        if (!response) {
            break;
        }
        this->length--;
        PyList_Append(this->array, response);
    }
    return this->length == 0 ? this->array : NULL;
}

#endif	//	__READIS_PARSER_H__
