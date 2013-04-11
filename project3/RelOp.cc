#include "RelOp.h"
#include "BigQ.h"
#include <sstream>
#include <fstream>

void* SF_Thread(void *sf_currentObj) {
    
        SelectFile obj = *((SelectFile *) sf_currentObj);

        obj.Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->sf_inputFile = &inFile;
    this->sf_outputPipe = &outPipe;
    this->sf_selectOperator = &selOp;
    this->sf_literalRecord = &literal;   
    
    int pthreadvar = pthread_create(&thread, NULL, &SF_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void SelectFile::Operation () {
	
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
    runLength = runlen;
}

void* P_Thread(void *p_currentObj) {
    
        Project obj = *((Project *) p_currentObj);

        obj.Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->p_inPipe = &inPipe;
    this->p_outPipe = &outPipe;
    this->p_keepMe = keepMe;
    this->p_numAttsInput = numAttsInput;
    this->p_numAttsOutput = numAttsOutput;
    
    int pthreadvar = pthread_create(&thread, NULL, &P_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void Project::Operation () {
    Record tempRec;
    while(p_inPipe->Remove(&tempRec))
    {
        tempRec.Project(p_keepMe,p_numAttsOutput,p_numAttsInput);
        p_outPipe->Insert(&tempRec);
    }
    p_outPipe->ShutDown();
}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
    runLength = runlen;
}


void* DR_Thread(void *dr_currentObj) {
    
        DuplicateRemoval obj = *((DuplicateRemoval *) dr_currentObj);

        obj.Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->dr_inPipe = &inPipe;
    this->dr_outPipe = &outPipe;
    this->dr_mySchema = &mySchema;
    
    int pthreadvar = pthread_create(&thread, NULL, &DR_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void DuplicateRemoval::Operation () {
    
    OrderMaker dr_orderMaker(dr_mySchema);
    Pipe sortedOut(PIPE_BUFF_SIZE);
    
    BigQarg arg={*dr_inPipe,sortedOut,dr_orderMaker,runLength};
    
    pthread_t BigQ_thread;
    
    int error = pthread_create(&BigQ_thread, NULL,BigQThread,(void *) &arg);
        
        if(error)
        {
            cout<<"\n Error while creating the BigQ Thread error# ";
        }
    
    Record rec,recNext;
    ComparisonEngine comp;
    
    if(sortedOut.Remove(&rec))
    {
        while(sortedOut.Remove(&recNext))
        {
            if(comp.Compare(&rec,&recNext,&dr_orderMaker)!=0)
            {
                dr_outPipe->Insert(&rec);
                rec=recNext;
            }
        }
        dr_outPipe->Insert(&rec);
    }
    
    dr_outPipe->ShutDown();
    
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
    runLength = runlen;
}


void* WO_Thread(void *wo_currentObj) {
    
        WriteOut obj = *((WriteOut *) wo_currentObj);

        obj.Operation();
        pthread_exit(NULL);
        //Peform a getnext to fetch the record from dbfile and keep a counter that increments when a page overflow
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    wo_inPipe = &inPipe;
    wo_outFile = outFile;
    wo_mySchema = &mySchema;
    
    int pthreadvar = pthread_create(&thread, NULL, &WO_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
        }
    
}

void WriteOut::Operation () {
    
    Record rec;
    
    while(wo_inPipe->Remove(&rec))
    {
        rec.PrintToFile(wo_outFile,wo_mySchema);
    }
    
}

void WriteOut::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
    runLength = runlen;
}

void* SP_Thread(void *sp_currentObj) {
    
        SelectPipe obj = *((SelectPipe *) sp_currentObj);

        obj.Operation();
        pthread_exit(NULL);
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    sp_inPipe = &inPipe;
    sp_outPipe = &outPipe;
    sp_selOp = &selOp;
    sp_literal = &literal;
    
    int pthreadvar = pthread_create(&thread, NULL, &SP_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
    }
    
}

void SelectPipe::Operation () {
    Record rec;
    ComparisonEngine comp;
    
    while(sp_inPipe->Remove(&rec)) {
        if(comp.Compare(&rec,sp_literal,sp_selOp))
            sp_outPipe->Insert(&rec);
    }
    sp_outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
    runLength = runlen;
}

void* GB_Thread(void *currentObj) {
    
        GroupBy obj = *((GroupBy *) currentObj);

        obj.Operation();
        pthread_exit(NULL);
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    g_inPipe = &inPipe;
    g_outPipe = &outPipe;
    g_groupAtts = &groupAtts;
    g_computeMe = &computeMe;
    
    int pthreadvar = pthread_create(&thread, NULL, &GB_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
    }
    
}

void GroupBy::Operation () {
    
}

void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
    runLength = runlen;
}

void* S_Thread(void *currentObj) {
    
        Sum obj = *((Sum *) currentObj);

        obj.Operation();
        pthread_exit(NULL);
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    s_inPipe = &inPipe;
    s_outPipe = &outPipe;
    s_computeMe = &computeMe;
    
    int pthreadvar = pthread_create(&thread, NULL, &S_Thread,(void *) this);
    if (pthreadvar) {
                printf("Error while creating a thread\n");
                exit(-1);
    }
    
}

void Sum::Operation () {
    
    Record rec,outRec;
    Type ret=Double; // Initialize to atleast print 0 and not give an error
    int intResult,finalIntResult=0;
    double doubleResult,finalDoubleResult=0;
    
    stringstream strResultStream;
    string str, out;
    char *name = "tempSumSchema";
    
    if(s_inPipe->Remove(&rec)) {
        ret = s_computeMe->Apply(rec,intResult,doubleResult);
        if(ret==Int) {
            finalIntResult+=intResult;
            while (s_inPipe->Remove(&rec)) {
                        s_computeMe->Apply(rec,intResult,doubleResult);
                        finalIntResult+=intResult;
                }
        }
        else if(ret==Double) {
            finalDoubleResult+=doubleResult;
            while (s_inPipe->Remove(&rec)) {
                        s_computeMe->Apply(rec,intResult,doubleResult);
                        finalDoubleResult+=doubleResult;
                }
        }
        else {
            cout<<"Wront type returned by Apply function in Function. Error called from Sum::Operation";
        }
    }
    
    outRec.bits =new (std::nothrow) char[sizeof(double)];
    
    s_inPipe->ShutDown();
        ofstream fout(name);
        fout << "BEGIN" << endl;
        fout << "SUM_table" << endl;
        fout << "sum.tbl" << endl;
        fout << "SUM ";
        if (ret == Int) {
                fout << "Int" << endl;
        } else {
                fout << "Double" << endl;
        }
        fout << "END";
        fout.close();
        Schema mySchema("tempSumSchema", "SUM_table");
        if (ret == Int) {
                strResultStream << finalIntResult;
                out = strResultStream.str();
                str.append(out);
                str.append("|");
        } else {
                strResultStream << finalDoubleResult;
                out = strResultStream.str();
                str.append(out);
                str.append("|");
        }
        const char*src = str.c_str();
        outRec.ComposeRecord(&mySchema, src);
        s_outPipe->Insert(&outRec);
        s_outPipe->ShutDown();
        remove(name);
}

void Sum::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {
    runLength = runlen;
}