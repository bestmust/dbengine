#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "Defs.h"

using namespace std;

typedef struct BigQArguments {
    Pipe &in,&out;
    OrderMaker &sortorder;
    int runlen;    
} BigQarg;

class BigQ {
private:
    pthread_t workerThread;
public:
#if MANUALTEST
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, Schema *);
#else
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);

#endif
    ~BigQ();
    
    void WaitUntilDone();
    
};

void *BigQThread(void *_arg);

#endif
