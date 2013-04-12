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



HeapDBFile::HeapDBFile() {
    metaFileName = NULL;
    numPages = 0;
    writePage=0;
}

int HeapDBFile::Create(char *f_path, fType f_type, void *startup) {

    //Bug Prone code
    //Create MetaFileName by appending .info to the binfile name given by f_path
    char *metaFileName;
    metaFileName = (char *) malloc(strlen(f_path) + 6);
    strcpy(metaFileName, f_path);
    strcat(metaFileName, ".info");

    //Copy the file Path to a variable in memory
    filePath = (char *) malloc(strlen(f_path));
    strcpy(filePath, f_path);

    //Set Current Record Position to 0, Current Page Position to Zero
    curRec = 0;
    curPage = 0;

    //Check if the file already exists if it does DO_NOT CREATE FILE;
    ifstream checkMeta(metaFileName, ios::in);
    if (checkMeta) {
        cerr << "File Already Exists Cannot Create the file";
        return 0;
    }
    checkMeta.close();

    //Opening the file that was created
    diskFile.Open(0, f_path);

    //Closing the File. Doing this will create a file of 0KB
    diskFile.Close();


    //Initializing the meta file info
    fileInfo.fileType = heap;

    //Write out the meta Info File
    ofstream metaFile(metaFileName, ios::binary);
    metaFile.write((char*) &fileInfo, sizeof (fileInfo));
    metaFile.close();

    return 1;
}

void HeapDBFile::Load(Schema &f_schema, char *loadpath) {

    //Opening the Disk File to Write to It.
    numPages = diskFile.GetLength();

    //Open the File to Bulk Load from; e.g. "lineitem.tbl"
    FILE *loadFile = fopen(loadpath, "r");

    Record temp;
    while (temp.SuckNextRecord(&f_schema, loadFile) == 1) {

        MDLog("MD:Record Sucked - ", curRec);

        if (inPage.Append(&temp) == 0) {
            // Write this Page to the file and create a new Page;
            diskFile.AddPage(&inPage, numPages);

            MDLog("MD:Page Added Inside", numPages);

            //Incrementing the Page offset
            numPages = numPages + 1;
            //Asserting where the page was consumed;
            inPage.EmptyItOut();
            //The current record read in will be written
            inPage.Append(&temp);
        }

        //Incrementing the Current record offset;
        curRec = curRec + 1;
    }

    //Add the Remaining Page to the File
    diskFile.AddPage(&inPage, numPages);

    numPages = numPages + 1;

    MDLog("MD:Last Page Added", numPages);
    MDLog("Record Offset = ", curRec);

    //Close the file that was opened for bulk loading
    fclose(loadFile);

    //Closing the diskFile

}

int HeapDBFile::Open(char *f_path) {

     //Copy the file Path to a variable in memory
    filePath = (char *) malloc(strlen(f_path));
    strcpy(filePath, f_path);

    ifstream binFile(f_path);
    if (binFile == 0)
        return 0;

    //Set the currently read record to zero
    curRec = 0;

    //Create the MetaFileName
    char *metaFileName;
    metaFileName = (char *) malloc(strlen(f_path) + 6);
    strcpy(metaFileName, f_path);
    strcat(metaFileName, ".info");

    ifstream metaFile(metaFileName, ios::binary);
    metaFile.read((char *) &fileInfo, sizeof (fileInfo));
    metaFile.close();

    diskFile.Open(1, f_path);
    numPages = diskFile.GetLength();

    MoveFirst();
    cout<<"heap file opened and move first called\n";
}

void HeapDBFile::MoveFirst() {

    

    if (curRec > 0) {


        //Reads the First Page into the File
        diskFile.GetPage(&inPage, 0);

        MDLog("First Page Read", NULL);

        //Moves the Current Page Pointer to Zero
        curPage = 1;

        //Moves the Current Rec Pointer to Zero
        curRec = 0;

    }
    cout<<"HeapDBFile: MoveFirst Success\n";
}

int HeapDBFile::Close() {
    //Write out the file Meta data to relation_name.inf

    //Distruct the DBFile

    diskFile.Close();
    return 1;
}
void HeapDBFile::Add(Record &rec) {
    //Open the diskFile
    //    diskFile.Open(1, filePath);

    //Read the Last Page of the file Not Sure if this is to be done
        
    if (numPages > 0 && writePage == 0) {
        //MDLog("Read In the Page -", numPages);
        diskFile.GetPage(&writePage1, numPages - 1);
    }

    //Write the Record specified in the argument int othe last page
    if (writePage1.Append(&rec) == 0) {

        writePage1.EmptyItOut();

        writePage1.Append(&rec);

        numPages = numPages + 1;

        MDLog("New Page is Created and added to file ", numPages);

    }

    //Add this page to the file
    diskFile.AddPage(&writePage1, numPages);

    //Close the diskFile
    //    diskFile.Close();
    writePage = numPages;
}

int HeapDBFile::GetNext(Record &fetchme) {

    //MDLog("Opening disk file",filePath);

    if (writePage == numPages) {
        MDLog("Page has become NULL", NULL);

        //Open the diskFile
        //Read the Current Page again
        diskFile.GetPage(&inPage, curPage - 1);

        //Go to the curRec in the page
        off_t goToCurrent = 0;
        while (goToCurrent < curRec - 1) {
            inPage.GetFirst(&fetchme);
            goToCurrent = goToCurrent + 1;
        }
        writePage = 0;
    }

    //Reads the current Record;
    if (inPage.GetFirst(&fetchme) == 0) {

        if (curPage >= diskFile.GetLength() - 1) {
            MDLog("Page Complete", diskFile.GetLength());
            return 0;
        } else {
            inPage.EmptyItOut();
            curRec = 0;
            curPage = curPage + 1;
            diskFile.GetPage(&inPage, curPage - 1);


        if(inPage.GetFirst(&fetchme)==0)
        return 0;
        }
    }

    //Close the diskFile
    //    diskFile.Close();
    MDLog("Reading Record ", curRec);
    MDLog("Reading Page ", curPage);
    curRec = curRec + 1;

    //Change the writePage to zero
    return 1;
}

int HeapDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while (GetNext(fetchme))
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}


