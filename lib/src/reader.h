#ifndef __READER_H
#define __READER_H

#include <Python.h>
#include "hiredis.h"


typedef struct pythonReader {
	PyObject *protocolErrorClass;
	PyObject *replyErrorClass;
	redisReader *reader;
	char *encoding;

	/* Stores error object in between incomplete calls to #gets, in order to
	 * only set the error once a full reply has been read. Otherwise, the
	 * reader could get in an inconsistent state. */
	struct {
		PyObject *ptype;
		PyObject *pvalue;
		PyObject *ptraceback;
	} error;
} pythonReader;


pythonReader* pythonReaderCreate(PyObject*, PyObject*);
void pythonReaderFree(pythonReader*);
PyObject* pythonReader_gets(pythonReader*);
void pythonReader_feed(pythonReader*, PyObject*);


#endif	//	__READER_H
