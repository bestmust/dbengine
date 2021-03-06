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
    
    //initialize the runlength
    mySortInfo = new SortInfo();
    
    //initialize current read page = 0
    curPage = 0;
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
//    curRec = 0;
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
    
    //initializing the ordermaker
    SortInfo *temp = (SortInfo *)startup;
    OrderMaker o = *temp->myOrder; 
    
    //Write out the filetype runlength and ordermaker in the Info File
    ofstream metaFile(metaFileName, ios::binary);
    metaFile.write((char*) &fileInfo, sizeof (fileInfo));
    metaFile.write((char*) &temp->runLength, sizeof (int));
    metaFile.write((char*) &o, sizeof (OrderMaker));
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
    
    //read the meta file contents namely filetype runlength and ordermaker.
    ifstream metaFile(metaFileName, ios::binary);
    metaFile.read((char *) &fileInfo, sizeof (fileInfo));
    metaFile.read((char *) &mySortInfo->runLength, sizeof (int));
    metaFile.read((char *) &o, sizeof (OrderMaker));
    metaFile.close();
    
    
    //check if the bin file exists.
    ifstream binFile(f_path);
    if (binFile == 0)
        return 0;

    //Set the currently read Page to zero
    curPage = 0;
    
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
        BigQarg arg = {*input,*output,o,length};
        
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
    
    this->WriteOut();
    
    inPage.EmptyItOut();
    
    curPage = 0;
    
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
        BigQarg arg = {*input,*output,o,length};
        
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
    
    if (fileMode == writing)
    {
        this->WriteOut();
        
        //change the file mode to reading.
        fileMode = reading;
        curPage = 0;
    
    } 
    
    if (diskFile.GetLength()>0) {
        
            //Reads the First Page into the File
            diskFile.GetPage(&inPage, 0);

            //Moves the Current Page Pointer to Zero
            curPage = 1;

            //Moves the Current Rec Pointer to Zero
            //curRec = 0;

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
    
    if (fileMode == writing)
    {
        this->WriteOut();
        
        //change the file mode to reading and initialize the next page to be read.
        fileMode = reading;
        curPage = 0;
        
        //actually read the first page.
        this->MoveFirst();
    }
    
    //if the file is empty output the error.
    if(numPages==0)
    {
        cout<<"\nGetNext: No records in the file."<<endl;
        return 0;
    }   
        
    //try to read the first record
    if(inPage.GetFirst(&fetchme)==0) {
            
        //check if we have reached the end of the file.
        if(curPage>=(numPages-1)) {
              cout<<"\nEnd of file\n";
                return 0;
        }      
        
        //read the new page in the memory
        diskFile.GetPage(&inPage,curPage);
        curPage = curPage + 1;       

        //get the first record from the new page.
        if(inPage.GetFirst(&fetchme)==0) {
            cout<<"\nThis line is executed in SortedFile GetNext because the last\n";
            cout<<"page was written on disk but it never had any records";
        }
        
    }
    
    //aim to remove the next line.
    //curRec = curRec + 1;
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
    
    //close the current file to avoid any problems
    diskFile.Close();
    
   //merge the output pipe with the current file.
    //open same file
    DBFile sameFile;
    sameFile.Open(filePath);
    sameFile.MoveFirst();
    
    Record rec;
    
    //read all records and insert into the in
    while(sameFile.GetNext(rec))
    {
        input->Insert(&rec);
    }
    
    //shutdown the input pipe so that BigQ can start second phase.
    input->ShutDown();
    //close the same file which was opened for reading.
    sameFile.Close(); 
    
    //again open the current file and write out the output. check if its fine to open the file as new
    diskFile.Open(1, filePath);
    
    //this it the temporary page used for writing to disk
    Page outPage;
    
    //start writing from the first page
    writePage = 0;
    
    while(output->Remove(&rec))
    {
        if(outPage.Append(&rec)==0) {

	    diskFile.AddPage(&outPage,writePage);
            
            outPage.EmptyItOut();
            
            outPage.Append(&rec);
            
            writePage = writePage + 1;
            
            MDLog("New Page is Created and added to file ", writePage);
            
        }
        
    }
        //write the last page to the disk.
        diskFile.AddPage(&outPage,writePage);
    
        //update the new number of Pages
        numPages = diskFile.GetLength();
        //shutdown the output pipe if everything is done.
        output->ShutDown();
        
        //delete the input and output pipes
        delete input;
        delete output;
}
