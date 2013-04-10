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
    
    int pthreadvar = pthread_create(&sf_thread, NULL, &SF_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void SelectFile::SF_Operation () {
	
    Record tempRec;
    
    while(sf_inputFile->GetNext(tempRec,*sf_selectOperator,*sf_literalRecord)) {
        sf_outputPipe->Insert(&tempRec);
        }
    
    sf_outputPipe->ShutDown();
    
}

void SelectFile::WaitUntilDone () {
	pthread_join (sf_thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    runLength = runlen;
}

void* P_Thread(void *p_currentObj) {
    
        Project obj = *((Project *) p_currentObj);

        obj.P_Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->p_inPipe = &inPipe;
    this->p_outPipe = &outPipe;
    this->p_keepMe = keepMe;
    this->p_numAttsInput = numAttsInput;
    this->p_numAttsOutput = numAttsOutput;
    
    int pthreadvar = pthread_create(&p_thread, NULL, &P_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void Project::P_Operation () {
    Record tempRec;
    while(p_inPipe->Remove(&tempRec))
    {
        tempRec.Project(p_keepMe,p_numAttsOutput,p_numAttsInput);
        p_outPipe->Insert(&tempRec);
    }
    p_outPipe->ShutDown();
}

void Project::WaitUntilDone () {
	pthread_join (p_thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
    runLength = runlen;
}

