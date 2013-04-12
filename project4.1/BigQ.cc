#include "BigQ.h"
#include "ComparisonEngine.h"
#include "File.h"
#include <vector>
#include <iterator>
#include<algorithm>
#include<cstdlib>
#include<ctime>
#include<stdlib.h>
#include<cstdio>
#include"Defs.h"

class comparableRecord {
    /* An Enccap sulation class to make the record comparable in the compvar function */
public:
    Record *r;
    OrderMaker *mySortOrder;
    int index;

    comparableRecord(Record *_r, OrderMaker *_mSO) {
        r = _r;
        mySortOrder = _mSO;
    }

    comparableRecord(Record *_r, OrderMaker *_mSO, int _index) {
        r = _r;
        mySortOrder = _mSO;
        index = _index;
    }
};

bool compvar(const void *one, const void *two) {
    /* Comparision funciton that is to be passed to the std::sort method*/
    comparableRecord *prev = (comparableRecord*) one;
    comparableRecord *last = (comparableRecord*) two;
    ComparisonEngine comparisonEngine;
    if ((comparisonEngine.Compare(prev->r, last->r, prev->mySortOrder)) < 0)
        return true;
    return false;
}

bool compvarheap(const void *one, const void *two) {
    /* Comparision funciton that is to be passed to the std::sort method*/
    comparableRecord *prev = (comparableRecord*) one;
    comparableRecord *last = (comparableRecord*) two;
    ComparisonEngine comparisonEngine;
    if ((comparisonEngine.Compare(prev->r, last->r, prev->mySortOrder)) < 0)
        return false;
    return true;
}

int writeOutRun(File &tmpFile, vector<comparableRecord*> &recsInRun, int locationOfRun) {
    /* Writes out a run to the tmpFile */
    Page putRecsHere;
    int loc = locationOfRun;
    for (int i = 0; i < recsInRun.size(); i++) {
        if (putRecsHere.Append(recsInRun[i]->r) == 0) {
            tmpFile.AddPage(&putRecsHere, loc++);
            putRecsHere.EmptyItOut();
            putRecsHere.Append(recsInRun[i]->r);
        }
    }
    tmpFile.AddPage(&putRecsHere, loc++);
    return loc;
}

class InputToRunReaderThreads {
public:
    File &file;
    pthread_mutex_t &fileMutex;
    pthread_mutex_t &pSizeMutex;
    int runLocation, runSize;
    Pipe &outPipe;
    int outInPipe;

    InputToRunReaderThreads(File &_f, pthread_mutex_t &_fM, pthread_mutex_t _pS, int _runLocation, int _runSize, Pipe &_out) : file(_f), fileMutex(_fM), pSizeMutex(_pS), outPipe(_out) {
        runLocation = _runLocation;
        runSize = _runSize;
    }
};

class Run {
private:
    bool runStarted;
    int runPos;
public:
    int index;
    Page buffer;
    int runLoc, size;

    Run(int _runLoc, int _size, int _ind) {
        runPos = _runLoc;
        runLoc = _runLoc;
        size = _size;
        runStarted = false;
        index = _ind;
    }

    int getNextRecord(File &f, Record *recordHere) {
        if (!runStarted) {
            runStarted = true;
            f.GetPage(&buffer, runPos++);
            buffer.GetFirst(recordHere);
        } else if (buffer.GetFirst(recordHere) == 0) {
            if (runPos < (runLoc + size)) {
                f.GetPage(&buffer, runPos++);
                buffer.GetFirst(recordHere);
            } else
                return 0;
        }
        return 1;
    }
};

void* RunReaderThread(void *inputToMe) {
    InputToRunReaderThreads &in = *((InputToRunReaderThreads*) inputToMe);
    int weReadAPage = 0;
    Page p;
    Record r;
    for (int i = in.runLocation; i < in.runLocation + in.runSize; i++) {
        cout << "\n" << in.runLocation << " -- ";
        pthread_mutex_lock(&(in.fileMutex));
        in.file.GetPage(&p, i);
        pthread_mutex_unlock(&(in.fileMutex));
        while (p.GetFirst(&r)) {

            pthread_mutex_lock(&(in.pSizeMutex));
            in.outPipe.Insert(&r);
            (in.outInPipe)++;
            pthread_mutex_unlock(&(in.pSizeMutex));
        }
        p.EmptyItOut();
    }
    in.outPipe.ShutDown();
    pthread_exit(NULL);
}

void *BigQThread(void *_arg) {
    cout << "\nBigQ: thread started";
    BigQarg *arg = (BigQarg *) _arg;
    BigQ bq(arg->in, arg->out, arg->sortorder, arg->runlen);
    cout << "\nBigQ: thread finished";
    pthread_exit(NULL);
}

class InPutToWorkerThread {
public:

    Pipe &in, &out;
    OrderMaker &sortorder;
    int runlen;

#if MANUALTEST
    Schema *tempSchema;

    InPutToWorkerThread(Pipe &_in, Pipe &_out, OrderMaker &_sortorder, int _runlen, Schema* _tempSchema) : in(_in), out(_out), sortorder(_sortorder) {

        runlen = _runlen;
        tempSchema = _tempSchema;
    }
#else

    InPutToWorkerThread(Pipe &_in, Pipe &_out, OrderMaker &_sortorder, int _runlen) : in(_in), out(_out), sortorder(_sortorder) {

        runlen = _runlen;
    }

#endif
};

void* QWorkerThread(void *inputToMe) {
#define RUNPHASE1 1
#define RUNPHASE2 1
    /*Initializing the local variables as ber the Input to the method
     *The InPutToWorkerThread is the object passed to the WT which contains encapsulated paraments
     *for the Big Q constructor */
    InPutToWorkerThread *inPut = (InPutToWorkerThread*) inputToMe;
    Pipe &out = (inPut->out);
    Pipe &in = (inPut->in);
#if MANUALTEST
    Schema *tempSchema = inPut->tempSchema;
#endif

    /*Creates a tmp file at ./tmp location with the filename as current timestamp*/
    File tmpFile;
    char *fileName = new char[20];
    sprintf(fileName, "./tmp/%d", std::time(0));
    tmpFile.Open(0, fileName);

    //    int recs = 0;
    //    Record *tgr = new Record();
    //    while (in.Remove(tgr)) {
    //        recs++;
    //        if (recs % 1000 == 0){
    //            cout << recs << " ";
    //            tgr->Print(tempSchema);
    //        }
    //        out.Insert(tgr);
    //        tgr = new Record();
    //    }
    //    cout <<"\n\nTOTAL "<< recs << " ";

#if RUNPHASE1
    OrderMaker &sortorder = (inPut->sortorder);
    const int RUNLENGHT = inPut->runlen;


    /*              LOCAL Variables declared on stack       */
    /**One readPage object to act as a container*/
    Page readPage;
    /*One read record to act as a temporary container*/
    Record readIntoPage;
    //pipeEmpty -- To check if the input pipe has depleted
    //runSize -- To keep track of how many pages of have been read.
    //pageHasSpace -- To Check if the page is full
    int pipeNotEmpty = 1, currentSizeOfRun = 0, pageHasSpace = 0;

    //Vector of Record pointers on which sorting is to be done
    std::vector<comparableRecord*> recPointerVector;

    //Iterator to Vector
    std::vector<Record*>::iterator vectorIterator;

    //RunLocations
    std::vector<int> runLocations;
    int numOfRuns = 0;

    //Temporary
    Record *tempCopyOfRecordFromPipe;
    comparableRecord *comparableRecordWhichGoesIntoVector;


    int recs = 0;


    /*Phase 1 ***********************************/
    /*Reads In Runs and Writes them out to a file
     Output of the below code is a file in ./tmp with fileName as timestamp
     and a vector<int> which has the offsetts to each of the runs*/
    pipeNotEmpty = in.Remove(&readIntoPage);
    while (pipeNotEmpty) {
        do {
            if (pipeNotEmpty && pageHasSpace) {
                recs++;
                pipeNotEmpty = in.Remove(&readIntoPage);
            }
            if (pipeNotEmpty) {
                //Generate a copy of the record;
                tempCopyOfRecordFromPipe = new Record();
                tempCopyOfRecordFromPipe->Copy(&readIntoPage);


                comparableRecordWhichGoesIntoVector = new comparableRecord(tempCopyOfRecordFromPipe, &sortorder);
                pageHasSpace = readPage.Append(&readIntoPage);
                if (pageHasSpace) {
                    recPointerVector.push_back(comparableRecordWhichGoesIntoVector);
                } else
                    delete tempCopyOfRecordFromPipe, comparableRecordWhichGoesIntoVector;
            }
        } while (pipeNotEmpty && pageHasSpace);

        //At this point one Page of data has been read Or the pipe has been depleted
        currentSizeOfRun++;
        readPage.EmptyItOut();

        //If the pipe has been depleted or the runSize has been reached Start Sorting
        if ((!pipeNotEmpty) || currentSizeOfRun == RUNLENGHT) {
            currentSizeOfRun = 0;
            //Sort the Run using the stl sort method.
            std::sort(recPointerVector.begin(), recPointerVector.end(), compvar);
            //If the pipe became empty in just one run then just out the sorted run
            if (!pipeNotEmpty && numOfRuns == 0) {
                for (int i = 0; i < recPointerVector.size(); i++) {
                    out.Insert(recPointerVector[i]->r);
                }
                recPointerVector.clear();
                out.ShutDown();
                tmpFile.Close();
                pthread_exit(NULL);
                return NULL;

            } else {
                /*Write the run to the file and store the offset*/
                if (numOfRuns == 0)
                    runLocations.push_back(writeOutRun(tmpFile, recPointerVector, 0));
                else
                    runLocations.push_back(writeOutRun(tmpFile, recPointerVector, runLocations.back()));
                numOfRuns++;
            }
            recPointerVector.clear();
        }
    }
    tmpFile.Close();
    cout << "\n\n PHASE ONE COMPLETE ----------- \n\n";
#endif


#if RUNPHASE2

    /*PHASE 2*/
    File runsFile;
    runsFile.Open(21, fileName);

    std::vector<Run*> buffRuns(runLocations.size(), NULL);
    for (int i = 0; i < runLocations.size(); i++) {
        int curLocation;
        if (i == 0)
            curLocation = 0;
        else
            curLocation = runLocations[i - 1];
        buffRuns[i] = new Run(curLocation, runLocations[i] - curLocation, i);
    }

    std::vector<comparableRecord*> bigQ(runLocations.size(), NULL);
    Record r;
    Page p;
    int c = 0;
    for (int i = 0; i < runLocations.size(); i++) {
        bigQ[i] = new comparableRecord(new Record(), &sortorder, buffRuns[i]->index);
        buffRuns[i]->getNextRecord(runsFile, bigQ[i]->r);
    }
    int index;
    Record *tempRecord;
    std::make_heap(bigQ.begin(), bigQ.end(), compvarheap);
    int numRecs = 0;
    while (!bigQ.empty()) {
        numRecs++;
        index = bigQ.front()->index;
        tempRecord = new Record();
        tempRecord->Consume(bigQ.front()->r);
        out.Insert(tempRecord);
        std::pop_heap(bigQ.begin(), bigQ.end(), compvarheap);
        bigQ.pop_back();
        if (buffRuns[index]->getNextRecord(runsFile, tempRecord)) {
            bigQ.push_back(new comparableRecord(tempRecord, &sortorder, index));
            std::push_heap(bigQ.begin(), bigQ.end(), compvarheap);
        }
    }

    //    Page inPage;
    //    int i = 0, fileLen = runsFile.GetLength();
    //    while (i < fileLen - 1) {
    //        runsFile.GetPage(&inPage, i++);
    //        while (inPage.GetFirst(&r)) {
    //
    //            r.Print(tempSchema);
    //            out.Insert(&r);
    //        }
    //        inPage.EmptyItOut();
    //    }
    runsFile.Close();

#endif

    //Delete the file
    std::remove(fileName);

    out.ShutDown();
    pthread_exit(NULL);
}
#if MANUALTEST

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, Schema* tempSchema) {
    pthread_t workerThread;
    InPutToWorkerThread *workOnThisData = new InPutToWorkerThread(in, out, sortorder, runlen, tempSchema);
#else

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    InPutToWorkerThread *workOnThisData = new InPutToWorkerThread(in, out, sortorder, runlen);
#endif

    pthread_create(&workerThread, NULL, QWorkerThread, (void*) workOnThisData);
    //Read page to determine how many pages have been read;
    void *status;

    /* do not wait for the thread to end.
    int rc = pthread_join(workerThread, &status);
        if (rc) {
         printf("ERROR; return code from pthread_join() is %d\n", rc);
         exit(-1);
         }
     */
}

BigQ::~BigQ() {
}

void BigQ::WaitUntilDone() {
    pthread_join(workerThread, NULL);
    cout<<"\nBigQ ended.\n";
}
