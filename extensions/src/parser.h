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
#include <list>

#define CRLF "\r\n"
#define RESPONSE_INTEGER  ':'
#define RESPONSE_STRING  '$'
#define RESPONSE_ARRAY  '*'
#define RESPONSE_STATUS  '+'
#define RESPONSE_ERROR  '-'


class Task;
class StringTask;
class ArrayTask;
typedef std::string string;
typedef long long integer;

class RedisParser {
public:
    RedisParser(PyObject *protocolError, PyObject *replyError):
        protocolError(protocolError), replyError(replyError), _current(NULL) {}

    ~RedisParser(){}
    //
    void feed(const char* data);
    void set_encoding(const char*);
    PyObject* get();
    PyObject* get_buffer() const;
private:
    Task* _current;
    PyObject *protocolError;
    PyObject *replyError;
    string encoding;
    string buffer;
    //
    PyObject* _get(Task* task);
    PyObject* decode(Task* task);
    PyObject* resume(Task* task, PyObject*);
    RedisParser();
    //
    friend class Task;
    friend class StringTask;
    friend class ArrayTask;
};


class Task {
public:
    Task(integer length, Task* next):length(length), next(next){}
    virtual ~Task() {}
    virtual PyObject* _decode(RedisParser& parser, PyObject*) = 0;
    Task* next;
protected:
    integer length;
private:
    Task();
    Task(const Task&);
};

class StringTask: public Task {
public:
    StringTask(integer length, Task* next):Task(length, next) {}
    PyObject* _decode(RedisParser& parser, PyObject*);
private:
    string str;
};


class ArrayTask: public Task {
public:
    ArrayTask(integer length, Task* next):Task(length, next), array(PyList_New(0)) {}
    PyObject* _decode(RedisParser& parser, PyObject*);
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

inline void RedisParser::set_encoding(const char* encoding) {
    this->encoding = encoding;
}

inline PyObject* RedisParser::get() {
    PyObject *result;
    if (this->_current) {
        result = this->resume(this->_current, NULL);
    } else {
        result = this->_get(NULL);
    }
    if (result) {
        return result;
    } else {
        Py_RETURN_FALSE;
    }
}

inline PyObject* RedisParser::_get(Task* next) {
    integer size = this->buffer.find(CRLF);
    if (size >= 0) {
        string response(buffer.substr(0, size));
        buffer.erase(0, size+2);
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
            return this->decode(new StringTask(atoi(response.c_str()), next));
        }
        case RESPONSE_ARRAY: {
            return this->decode(new ArrayTask(atoi(response.c_str()), next));
        }
        default:
            this->buffer.clear();
            PyErr_SetString(this->protocolError, "protocol error");
            return NULL;
        }
    } else {
        this->_current = next;
        return NULL;
    }
}

inline PyObject* RedisParser::decode(Task* task) {
    PyObject* result = task->_decode(*this, NULL);
    if (result) {
        delete task;
    }
    return result;
}

inline PyObject* RedisParser::resume(Task* task, PyObject* result) {
    result = task->_decode(*this, result);
    if (result) {
        if (task->next) {
            result = this->resume(task, result);
        }
        delete task;
    }
    return result;
}

inline PyObject* RedisParser::get_buffer() const {
    return pybytes(this->buffer);
}

inline PyObject* StringTask::_decode(RedisParser& parser, PyObject* result) {
    if (this->length == -1) {
        return Py_BuildValue("");
    } else if (parser.buffer.size() >= this->length+2) {
        parser._current = NULL;
        PyObject* result;
        if (parser.encoding.size()) {
            result = PyUnicode_Decode(
                    parser.buffer.substr(0, this->length).c_str(),
                    this->length, parser.encoding.c_str(), NULL);
        } else {
            result = pybytes(parser.buffer.substr(0, this->length));
        }
        parser.buffer.erase(0, this->length+2);
        return result;
    } else {
        parser._current = this;
        return NULL;
    }
}

inline PyObject* ArrayTask::_decode(RedisParser& parser, PyObject* result) {
    if (result) {
        this->length--;
        PyList_Append(this->array, result);
    }
    while (this->length > 0) {
        result = parser._get(this);
        if (!result) {
            break;
        }
        this->length--;
        PyList_Append(this->array, result);
    }
    return this->length == 0 ? this->array : NULL;
}

#endif	//	__READIS_PARSER_H__
