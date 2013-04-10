/* 
 * File:   Sorted.h
 * Author: mustaqim
 *
 * Created on April 4, 2013, 1:07 AM
 */

#ifndef SORTED_H
#define	SORTED_H

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
#include<cstdlib>
#include<time.h>
#include <algorithm>

#define MANUALDEBUG 1

#if MANUALDEBUG
#define MDLog(x,y)  ( cout<<"\n"<<x<<" - "<<y )
#else
#define MDLog(x,y) ( cout<<"" )
#endif

class SortedDBFile: public GenericDBFile {
private:
    
    //for closing the input pipe and writing the file.
    void WriteOut();
   
public:

    //Internal BigQ thread function
    //void *internalBigQ(void *arg);
    SortedDBFile();

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


#endif	/* SORTED_H */

