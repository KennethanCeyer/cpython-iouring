#include <Python.h>
#include <liburing.h>
#include <fcntl.h>
#include <unistd.h>

typedef struct {
    PyObject_HEAD
    int fd;
    struct io_uring ring;
} IOUringObject;

static PyObject* IOUring_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    IOUringObject* self;
    self = (IOUringObject*)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->fd = -1;
    }
    return (PyObject*)self;
}

static int IOUring_init(IOUringObject* self, PyObject* args, PyObject* kwds) {
    const char* path;
    if (!PyArg_ParseTuple(args, "s", &path))
        return -1;

    self->fd = open(path, O_RDONLY);
    if (self->fd < 0) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
        return -1;
    }

    if (io_uring_queue_init(8, &self->ring, 0) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_queue_init failed");
        close(self->fd);
        return -1;
    }

    return 0;
}

static void IOUring_dealloc(IOUringObject* self) {
    if (self->fd >= 0)
        close(self->fd);
    io_uring_queue_exit(&self->ring);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* IOUring_read(IOUringObject* self, PyObject* Py_UNUSED(ignored)) {
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    PyObject* result;
    struct stat st;
    off_t file_size;
    char* buffer;

    if (fstat(self->fd, &st) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    file_size = st.st_size;

    buffer = PyMem_Malloc(file_size);
    if (!buffer) {
        PyErr_NoMemory();
        return NULL;
    }

    sqe = io_uring_get_sqe(&self->ring);
    io_uring_prep_read(sqe, self->fd, buffer, file_size, 0);

    if (io_uring_submit(&self->ring) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_submit failed");
        PyMem_Free(buffer);
        return NULL;
    }

    if (io_uring_wait_cqe(&self->ring, &cqe) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_wait_cqe failed");
        PyMem_Free(buffer);
        return NULL;
    }

    if (cqe->res < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        PyMem_Free(buffer);
        io_uring_cqe_seen(&self->ring, cqe);
        return NULL;
    }

    result = PyBytes_FromStringAndSize(buffer, cqe->res);
    PyMem_Free(buffer);
    io_uring_cqe_seen(&self->ring, cqe);
    return result;
}

static PyObject* IOUring_print(IOUringObject* self, PyObject* Py_UNUSED(ignored)) {
    PyObject* data = IOUring_read(self, NULL);
    if (!data)
        return NULL;

    if (PySys_WriteStdout("%s", PyBytes_AS_STRING(data)) < 0) {
        Py_DECREF(data);
        return NULL;
    }

    Py_DECREF(data);
    Py_RETURN_NONE;
}

static PyMethodDef IOUring_methods[] = {
    {"read", (PyCFunction)IOUring_read, METH_NOARGS, "Read data from the file"},
    {"print", (PyCFunction)IOUring_print, METH_NOARGS, "Print data to stdout"},
    {NULL}  /* Sentinel */
};

static PyTypeObject IOUringType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "asyncio.io_uring.IOUring",
    .tp_doc = "IOUring objects",
    .tp_basicsize = sizeof(IOUringObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = IOUring_new,
    .tp_init = (initproc)IOUring_init,
    .tp_dealloc = (destructor)IOUring_dealloc,
    .tp_methods = IOUring_methods,
};

static PyModuleDef _asynciouringmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_asynciouring",
    .m_doc = "C extension module for asyncio io_uring support",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit__asynciouring(void) {
    PyObject* m;
    if (PyType_Ready(&IOUringType) < 0)
        return NULL;

    m = PyModule_Create(&_asynciouringmodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&IOUringType);
    if (PyModule_AddObject(m, "IOUring", (PyObject*)&IOUringType) < 0) {
        Py_DECREF(&IOUringType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
