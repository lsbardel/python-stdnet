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
#include <cstdlib>
#include <string>
#include <sstream>
#include <deque>

#define CR '\r'
#define PROTOCOL_ERROR  ''
#define RESPONSE_INTEGER  ':'
#define RESPONSE_STRING  '$'
#define RESPONSE_ARRAY  '*'
#define RESPONSE_STATUS  '+'
#define RESPONSE_ERROR  '-'


class Task;
class StringTask;
class ArrayTask;
typedef std::shared_ptr<Task> TaskPtr;
typedef std::stringstream  buffer_type;
typedef std::string str_type;
typedef long long integer;

class RedisParser {
public:
    RedisParser(PyObject *protocolError, PyObject *replyError):
        protocolError(protocolError), replyError(replyError) {}

    ~RedisParser(){}
    //
    void feed(const char* data);
    PyObject* get();
private:
    std::deque<TaskPtr> stack;
    PyObject *protocolError;
    PyObject *replyError;
    buffer_type buffer;
    //
    PyObject* task(TaskPtr& task);
    PyObject* get_from_stack();
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
    str_type str;
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
inline PyObject* pystring(const str_type& value) {
    return Py_BuildValue("s#", value.c_str(), value.size());
}
//
inline PyObject* pystring_tuple(const str_type& value) {
    return Py_BuildValue("(s#)", value.c_str(), value.size());
}
//
inline PyObject* pylong(const str_type& value) {
    long long resp = atoi(value.c_str());
    return PyLong_FromLongLong(resp);
}

//
// read data from the buffer and put it into string str. If the data is ready
// return true, otherwise false.
inline bool read_buffer(buffer_type& buffer, str_type& str) {
    std::getline(buffer, str);
    if (buffer.eof()) {
        buffer.clear();
        buffer.seekg(0, buffer.beg);
        return false;
    } else {
        size_t s = str.size();
        // done with it
        if (s && str.at(s-1) == CR) {
            str.pop_back();
            return true;
        } else {    // more data in the string
            str_type extra;
            if (read_buffer(buffer, extra)) {
                str += extra;
                return true;
            } else {
                return false;
            }
        }
    }
}


inline void RedisParser::feed(const char* data) {
    buffer << data;
}

inline PyObject* RedisParser::get() {
    str_type response;
    if (read_buffer(this->buffer, response)) {
        char rtype(response.at(1));
        response.erase(0,1);
        switch(rtype) {
        case RESPONSE_STATUS:
            return pystring(response);
        case RESPONSE_INTEGER:
            return pylong(response);
        case RESPONSE_ERROR: {
            PyObject* args = pystring_tuple(response);
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

inline PyObject* RedisParser::get_from_stack() {
    if (this->stack.size()) {
        TaskPtr task(this->stack.front());
        this->stack.pop_front();
        PyObject* result = task->decode(*this);
        if (!result) {
            this->stack.push_front(task);
        }
        return result;
    } else {
        return this->get();
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
    str_type str;
    if(read_buffer(parser.buffer, str)) {
        this->str += str;
        if (this->length == this->str.size()) {
            return pystring(this->str);
        }
    }
    return NULL;
}

inline PyObject* ArrayTask::decode(RedisParser& parser) {
    PyObject* response;
    while (this->length > 0) {
        response = parser.get_from_stack();
        if (!response) {
            break;
        }
        this->length--;
        PyList_Append(this->array, response);
    }
    return this->length == 0 ? this->array : NULL;
}

#endif	//	__READIS_PARSER_H__
