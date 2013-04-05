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
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <fstream>



SortedDBFile::SortedDBFile() {
    
    //metaFile will be created only after create()
    metaFileName = NULL;
    
    //initialize total number of pages to zero
    numPages = 0;
    
    //initialize written pages to zero
    writePage=0;
    
    //let the file be in reading mode initially.
    fileMode= reading;
}

int SortedDBFile::Create(char *f_path, fType f_type, void *startup) {

    //Create MetaFileName by appending .info to the binfile name given by f_path
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
        cerr << "\nFile Already Exists Cannot Create the file";
        return 0;
    }
    checkMeta.close();

    //Opening the file that was created
    diskFile.Open(0, f_path);

    //Closing the File. Doing this will create a file of 0KB
    diskFile.Close();

    //Initializing the meta file info
    fileInfo.fileType = sorted;

    //Write out the meta Info File
    ofstream metaFile(metaFileName, ios::binary);
    metaFile.write((char*) &fileInfo, sizeof (fileInfo));
    metaFile.write((char*) &startup, sizeof (SortInfo));
    metaFile.close();

    return 1;
}


int SortedDBFile::Open(char *f_path) {
    
    //SortInfo startup;

    //Copy the file Path to a variable in memory
    filePath = (char *) malloc(strlen(f_path));
    strcpy(filePath, f_path);
    
    //Create the MetaFileName
    metaFileName = (char *) malloc(strlen(f_path) + 6);
    strcpy(metaFileName, f_path);
    strcat(metaFileName, ".info");
    
    //read the meta file contents
    ifstream metaFile(metaFileName, ios::binary);
    metaFile.read((char *) &fileInfo, sizeof (fileInfo));
    metaFile.read((char *) &mySortInfo, sizeof (SortInfo));
    metaFile.close();

    //check if the file exists.
    ifstream binFile(f_path);
    if (binFile == 0)
        return 0;

    //Set the currently read record to zero
    curRec = 0;
    
    //open the file in the reading mode.
    fileMode = reading;
    
    //actually open the file
    diskFile.Open(1, f_path);
    
    //get the number of pages in the file;
    numPages = diskFile.GetLength();
    
    //move to the first record of the file
    MoveFirst();
}

void SortedDBFile::Load(Schema &f_schema, char *loadpath) {
    
    //Opening the Disk File to Write to It.
    numPages = diskFile.GetLength();

    //Open the File to Bulk Load from; e.g. "lineitem.tbl"
    FILE *loadFile = fopen(loadpath, "r");
    
    Record temp;
    /*if the file mode is reading then the input and output pipes 
     * should be initialized and then the BigQ thread should be created.
     * after all the input is put into input pipe the input pipe
     * should be shutdown and the output from output pipe should be
     * written to the diskfile.
     * then both the input and output pipes should be destroyed.
     */ 
     if(fileMode == reading)
    {
         //initializing the input and output pipe.
        input = new Pipe(PIPE_BUFF_SIZE);
        output = new Pipe(PIPE_BUFF_SIZE);
         //runlength for the bigQ
        int length = mySortInfo->runLength;
        
        //initialize and create the BigQ thread.   
        BigQarg arg = {*input,*output,*mySortInfo->myOrder,length};
        
        //now create the BigQ thread to sort the input.
        pthread_create(&BigQ_thread, NULL,BigQThread,(void *) &arg);
        
        //putting all the input into the input pipe.
        while (temp.SuckNextRecord(&f_schema, loadFile) == 1) 
        {
               input->Insert(&temp);
        }
        
    }
    else if (fileMode==writing)
    {
        while (temp.SuckNextRecord(&f_schema, loadFile) == 1) 
        {       
                input->Insert(&temp);
        }
        fileMode = reading;
    }
    
    //Close the file that was opened for bulk loading
    fclose(loadFile);
    
    //shutdown the input pipe so that the BigQ can start the second phase.
    input->ShutDown();
    
    //suck the records from output pipe and save them in the file.
    while (output->Remove(&temp)) {

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
    
    //destroy the input and output pipes.
    delete input;
    delete output;
}

void SortedDBFile::Add(Record &rec) {
    
    /*if the fileMode is reading then the input and output pipe 
     * should be initialized and a bigQ thread should be created to
     * sort the coming input
     */
    if(fileMode == reading)
    {
        int error;
        //initializing the input and output pipe.
        input = new Pipe(PIPE_BUFF_SIZE);
        output = new Pipe(PIPE_BUFF_SIZE);
        
        //insert the record in the input pipe.
        input->Insert(&rec);
        
        //runlength for the bigQ
        int length = mySortInfo->runLength;
        
        //initialize and create the BigQ thread.   
        BigQarg arg = {*input,*output,*mySortInfo->myOrder,length};
        
        //now create the BigQ thread to sort the input.
        error = pthread_create(&BigQ_thread, NULL,BigQThread,(void *) &arg);
        
        if(error)
        {
            cout<<"\n Error while creating the BigQ Thread error# ";
        }
        
        //set the file mode to writing.
        fileMode = writing;
    }
    else if (fileMode==writing)
    {
        input->Insert(&rec);
    }
}

void SortedDBFile::MoveFirst() {

    if(fileMode == reading)
    {
        if (curRec>0) {

            //Reads the First Page into the File
            diskFile.GetPage(&inPage, 0);

            MDLog("First Page Read", NULL);

            //Moves the Current Page Pointer to Zero
            curPage = 1;

            //Moves the Current Rec Pointer to Zero
            curRec = 0;

        }
    }
    else if (fileMode == writing)
    {
        this->WriteOut();
        
        //change the file mode to reading.
        fileMode = reading;
    }

}

int SortedDBFile::Close() {
    
    cout<<"SortedDBFile: Trying to close the file.";
     
    if(fileMode == writing)
    {
        this->WriteOut();
    }
  
    diskFile.Close();
    return 1;
}

int SortedDBFile::GetNext(Record &fetchme) {
    
    //ToDo: implement the change of read and write modes here.

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

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    
    //write the improvised version of this function.
    
    ComparisonEngine comp;
    while (GetNext(fetchme))
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}

void SortedDBFile::WriteOut() {
    
    input->ShutDown();
         
    Record rec;
    
    while(output->Remove(&rec))
    {
        //MDLog("Read In the Record ", NULL);
    
    if (numPages > 0 && writePage == 0) {
        MDLog("Read In the Page -", numPages);
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
           //shutdown the output pipe if everything is done.
        output->ShutDown();
        
        //delete the input and output pipes
        delete input;
        delete output;
    
}