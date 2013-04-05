#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Pipe.h"
#include "Sorted.h"
#include "Heap.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <fstream>

#define MANUALDEBUG 1

#if MANUALDEBUG
#define MDLog(x,y)  ( cout<<"\n"<<x<<" - "<<y )
#else
#define MDLog(x,y) ( cout<<"" )
#endif

// stub file .. replace it with your own DBFile.cc


int DBFile::Create(char *f_path, fType f_type, void *startup) {
    
    int ret;

    //crete either heap, sorted or Btree file
    if (f_type == heap)
    {
        myInternalVar = new HeapDBFile();
    }
    else if (f_type = sorted)
    {
        myInternalVar = new SortedDBFile();
    }
    else
    {
        // code for B tree file
    }
        
    ret = myInternalVar->Create(f_path,f_type,startup);
    
    if(!ret)
    {
        cout<<"\nError while creating file";
        exit(0);
    }
    
    return 1;
}

void DBFile::Load(Schema &f_schema, char *loadpath) {

    myInternalVar->Load(f_schema,loadpath);

}

int DBFile::Open(char *f_path) {
    
    fType f_type;
    Meta fileInfo;
    
    //Create the MetaFileName string
    char *metaFileName;
    metaFileName = (char *) malloc(strlen(f_path) + 6);
    strcpy(metaFileName, f_path);
    strcat(metaFileName, ".info");

    //extract the file information from the medaFile
    ifstream metaFile(metaFileName, ios::binary);
    metaFile.read((char *) &fileInfo, sizeof (fileInfo));
    metaFile.close();
    
    //save the file type
    f_type = fileInfo.fileType;

    if (f_type == heap)
    {
        myInternalVar = new HeapDBFile();
    }
    else if (f_type == sorted)
    {
        myInternalVar = new SortedDBFile();
    }
    else
    {
        //code to create Btree file object
    }
    
    int ret;
    ret = myInternalVar->Open(f_path);
    if(!ret)
    {
        cout<<"\nError while opening the file";
        exit(0);
    }
    
    return ret;
    
}

void DBFile::MoveFirst() {
   
    myInternalVar->MoveFirst();
   
}

int DBFile::Close() {
    
    int ret;
    
    ret = myInternalVar->Close();
    
    delete myInternalVar;
    
    return ret;
}

void DBFile::Add(Record &rec) {
    
    myInternalVar->Add(rec);

}

int DBFile::GetNext(Record &fetchme) {

    return myInternalVar->GetNext(fetchme);

}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    
    return myInternalVar->GetNext(fetchme,cnf,literal);
    
}
