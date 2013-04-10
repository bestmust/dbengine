#include "RelOp.h"

void* SF_Thread(void *sf_currentObj) {
    
        SelectFile obj = *((SelectFile *) sf_currentObj);

        obj.SF_Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->sf_inputFile = &inFile;
    this->sf_outputPipe = &outPipe;
    this->sf_selectOperator = &selOp;
    this->sf_literalRecord = &literal;   
    
    int pthreadvar = pthread_create(&thread, NULL, &SF_Thread,(void *) this);
    
}

void SelectFile::SF_Operation () {
	
    Record tempRec;
    
    while(sf_inputFile->GetNext(tempRec,*sf_selectOperator,*sf_literalRecord)) {
        sf_outputPipe->Insert(&tempRec);
    }
    
    sf_outputPipe->ShutDown();
    
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    sf_runlen = runlen;
}

