#ifndef DBFILE_H
#define DBFILE_H


#include "Pipe.h"
#include "BigQ.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <stdio.h>
#include <vector>
#include <cstdlib>
#include <time.h>
#include <algorithm>

struct SortInfo {
OrderMaker *myOrder;
int runLength;
};

// stub DBFile header..replace it with your own DBFile.h

class GenericDBFile {
protected:
    //Page pointer that was/will be currently written
    off_t writePage;

    //Current Record Pointer. currently it is the absolute record pointer. if possible change it to the relative
    //record pointer in the page.
    off_t curRec;

    //Current Page
    off_t curPage;

    //Number of Pages in File
    //REMEMBER: the number of pages will always be 1 more than than pages containing data.
    //because the first page contains extra information and no data.
    off_t numPages;

    //File Object that will be used to store the Pages on the disk
    File diskFile;

    //In Memory Page that will hold the records until they are written out
    Page inPage,writePage1;

    //Meta Data Object
    Meta fileInfo;

    //Temporarily Store the Path of the File
    char *filePath;

    //Temporarily Store the Path of the metaFile
    char *metaFileName;
    
    //for sorted file. use only the file length. so not use ordermaker.
    SortInfo *mySortInfo;
    
    Mode fileMode;
  
    //Internal BigQ thread id
    pthread_t BigQ_thread;
    
    //this is the main important file ordermaker
    OrderMaker o;
    

public:
     //Pipes used by the internal BigQ instance.
    Pipe *input;
    Pipe *output;
    
    //Creates a new DBFile
    //fpath- path at which the file is to be placed.
    //filetype - either heap, sorted, tree   This will decide the implementation
    //returns -1 if file was not created.
    virtual int Create(char *fpath, fType file_type, void *startup) = 0;

    //Opens an existing file
    virtual int Open(char *fpath) = 0;
    virtual  int Close() = 0;

    virtual void Load(Schema &myschema, char *loadpath) = 0;

    virtual void MoveFirst() = 0;
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

    //Scramble File -- This function is just for testing purpose
    void scramble(Schema*);

};


class DBFile {

private:
    
    GenericDBFile *myInternalVar;
    
public:

    //DBFile();

    //Creates a new DBFile
    //fpath- path at which the file is to be placed.
    //filetype - either heap, sorted, tree   This will decide the implementation
    //returns -1 if file was not created.
    int Create(char *fpath, fType file_type, void *startup);

    //Opens an existing file
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

#endif