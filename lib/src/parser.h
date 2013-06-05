#ifndef __READER_H
#define __READER_H

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
    PyObject* gets();
    //
    TaskPtr task(bool=false);
    //
    PyObject *protocolError;
    PyObject *replyError;
    //
    buffer_type buffer;
    std::deque<TaskPtr> stack;
    //
    RedisParser();
};
//
// Obatin a python string from a c++ stringstream buffer
inline PyObject* pystring(const str_type& value) {
    return Py_BuildValue("(s#)", value.c_str(), value.size());
}

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

class Task {
public:
    Task():_response(NULL){}
    virtual ~Task(){}
    //
    virtual PyObject* decode(RedisParser& parser) {
        return this->_response;
    }
protected:
    PyObject *_response;
};

template<char>
class RedisReadTask: public Task {
public:
    RedisReadTask(RedisParser& parser, const str_type& response) {
        this->_response = PyObject_CallObject(parser.protocolError);
    }
};


template<>
class RedisReadTask<RESPONSE_ERROR>: public Task {
public:
    RedisReadTask(RedisParser& parser, const str_type& response) {
        PyObject* s = pystring(response);
        this->_response = PyObject_CallObject(parser.replyError, s);
    }
};

template<>
class RedisReadTask<RESPONSE_INTEGER>: public Task {
public:
    //
    RedisReadTask(RedisParser& parser, const str_type& response) {
        long long resp = atoi(response.c_str());
        this->_response = PyLong_FromLongLong(resp);
    }
};

template<>
class RedisReadTask<RESPONSE_STATUS>: public Task {
public:
    //
    RedisReadTask(RedisParser& parser, const str_type& response) {
        this->_response = pystring(response);
    }
};


template<>
class RedisReadTask<RESPONSE_STRING>: public Task {
public:
    RedisReadTask(RedisParser& parser, const str_type& response) {
        this->length = atoi(response.c_str());
    }
    PyObject* decode(RedisParser& parser);
private:
    long long length;
};


template<>
class RedisReadTask<RESPONSE_ARRAY>: public Task {
public:
    RedisReadTask(RedisParser& parser, const str_type& response) {
        this->length = atoi(response.c_str());
        this->_response = PyList_New(0);
    }
    PyObject* decode(RedisParser& parser);
private:
    long long length;
};


inline void RedisParser::feed(const char* data) {
    buffer << data;
}

inline PyObject* RedisParser::gets() {
    TaskPtr task(this->task());
    if (task) {
        PyObject* result = task->decode(*this);
        if (!result) {
            this->stack.push_back(task);
            return NULL;
        } else {
            return result;
        }
    } else {
        return NULL;
    }
}

inline TaskPtr RedisParser::task(bool recursive=false) {
    TaskPtr task;
    if (this->stack.size() && !recursive) {
        return this->_stack.pop_front();
    } else {
        // Either recursive call or no stack available.
        string_type str;
        if (read_buffer(this->buffer, str)) {
            char rtype(str.at(1));
            str.erase(0,1);
            switch(rtype) {
            case RESPONSE_STATUS:
                return TaskPtr(new redisReadTask<RESPONSE_STATUS>(*this, response));
            case RESPONSE_INTEGER:
                return TaskPtr(new redisReadTask<RESPONSE_INTEGER>(*this, response));
            case RESPONSE_STRING:
                return TaskPtr(new redisReadTask<RESPONSE_STRING>(*this, response));
            case RESPONSE_ARRAY:
                return TaskPtr(new redisReadTask<RESPONSE_ARRAY>(*this, response.str()));
            case RESPONSE_STATUS:
                return TaskPtr(new redisReadTask<RESPONSE_STATUS>(*this, response));
            case RESPONSE_ERROR:
                return TaskPtr(new redisReadTask<RESPONSE_ERROR>(*this, response));
            default:
                return TaskPtr(new redisReadTask<''>(*this, response));
            }
        } else {
            return NULL;
        }
    }
}


inline PyObject* RedisReadTask<RESPONSE_STRING>:decode(RedisParser& parser) {
    string_type str;
    if(read_buffer(parser.buffer, str)) {
        this->_response = pystring(str);
    }
    return this->response;
}

inline PyObject* RedisReadTask<RESPONSE_ARRAY>:response(RedisParser& parser) {
    TaskPtr task;
    PyObject* response;
    while (this->length > 0) {
        task = parser->task(true);
        if (!task) {
            break;
        }
        response = task->response(parser);
        if (!response) {
            parser.stack.push_back(task);
            break;
        }
        this->length--;
        PyList_Append(this->_response, response);
    }
    return this->length == 0 ? this->_response : NULL;
}

#endif	//	__READER_H
