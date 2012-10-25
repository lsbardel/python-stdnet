#include "reader.h"

static void *tryParentize(const redisReadTask *task, PyObject *obj) {
    PyObject *parent;
    if (task && task->parent) {
        parent = (PyObject*)task->parent->obj;
        assert(PyList_CheckExact(parent));
        PyList_SET_ITEM(parent, task->idx, obj);
    }
    return obj;
}

static PyObject *createDecodedString(pythonReader *self, const char *str, size_t len) {
    PyObject *obj;

    if (self->encoding == NULL) {
        obj = PyBytes_FromStringAndSize(str, len);
    } else {
        obj = PyUnicode_Decode(str, len, self->encoding, NULL);
        if (obj == NULL) {
            if (PyErr_ExceptionMatches(PyExc_ValueError)) {
                /* Ignore encoding and simply return plain string. */
                obj = PyBytes_FromStringAndSize(str, len);
            } else {
                assert(PyErr_ExceptionMatches(PyExc_LookupError));

                /* Store error when this is the first. */
                if (self->error.ptype == NULL)
                    PyErr_Fetch(&(self->error.ptype), &(self->error.pvalue),
                            &(self->error.ptraceback));

                /* Return Py_None as placeholder to let the error bubble up and
                 * be used when a full reply in Reader#gets(). */
                obj = Py_None;
                Py_INCREF(obj);
            }

            PyErr_Clear();
        }
    }

    assert(obj != NULL);
    return obj;
}

static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
    pythonReader *self = (pythonReader*)task->privdata;
    PyObject *obj;

    if (task->type == REDIS_REPLY_ERROR) {
        PyObject *args = Py_BuildValue("(s#)", str, len);
        assert(args != NULL); /* TODO: properly handle OOM etc */
        obj = PyObject_CallObject(self->replyErrorClass, args);
        assert(obj != NULL);
        Py_DECREF(args);
    } else {
        obj = createDecodedString(self, str, len);
    }

    return tryParentize(task, obj);
}

static void *createArrayObject(const redisReadTask *task, int elements) {
    PyObject *obj;
    obj = PyList_New(elements);
    return tryParentize(task, obj);
}

static void *createIntegerObject(const redisReadTask *task, long long value) {
    PyObject *obj;
    obj = PyLong_FromLongLong(value);
    return tryParentize(task, obj);
}

static void *createNilObject(const redisReadTask *task) {
    PyObject *obj = Py_None;
    Py_INCREF(obj);
    return tryParentize(task, obj);
}

static void freeObject(void *obj) {
    Py_XDECREF(obj);
}

redisReplyObjectFunctions PyObjectFunctions = {
    createStringObject,  // void *(*createString)(const redisReadTask*, char*, size_t);
    createArrayObject,   // void *(*createArray)(const redisReadTask*, int);
    createIntegerObject, // void *(*createInteger)(const redisReadTask*, long long);
    createNilObject,     // void *(*createNil)(const redisReadTask*);
    freeObject           // void (*freeObject)(void*);
};


void pythonReaderFree(pythonReader *self) {
	if(self != NULL) {
		redisReaderFree(self->reader);
		if (self->encoding)
			free(self->encoding);
		free(self);
	}
}


pythonReader* pythonReaderCreate(PyObject *protocolErrorClass,
								 PyObject *replyErrorClass) {
	pythonReader *self;
	redisReader *r;

	r = redisReaderCreate();
	if (r == NULL)
		return NULL;

	self = calloc(sizeof(pythonReader),1);
	if (self == NULL) {
		redisReaderFree(r);
		return NULL;
	}
	self->protocolErrorClass = protocolErrorClass;
	self->replyErrorClass = replyErrorClass;
	self->encoding = NULL;
	self->reader = r;
	self->reader->fn = &PyObjectFunctions;
	self->reader->privdata = self;
	self->error.ptype = NULL;
	self->error.pvalue = NULL;
	self->error.ptraceback = NULL;
	return self;
}


void pythonReader_feed(pythonReader *self, PyObject *args) {
	const char *str;
	int len;
	if (!PyArg_ParseTuple(args, "s#", &str, &len))
		return;
	redisReplyReaderFeed(self->reader, str, len);
}

PyObject* pythonReader_gets(pythonReader* self) {
	PyObject *obj;
	char *err;

	// we got an error
	if(redisReaderGetReply(self->reader, (void**)&obj) == REDIS_ERR) {
		err = redisReplyReaderGetError(self->reader);
		PyErr_SetString(self->protocolErrorClass, err);
		return NULL;
	}
	if (obj == NULL) {
		Py_RETURN_FALSE;
	} else {
		/* Restore error when there is one. */
		if (self->error.ptype != NULL) {
			Py_DECREF(obj);
			PyErr_Restore(self->error.ptype, self->error.pvalue,
							self->error.ptraceback);
			self->error.ptype = NULL;
			self->error.pvalue = NULL;
			self->error.ptraceback = NULL;
			return NULL;
		}
		return obj;
	}
}

