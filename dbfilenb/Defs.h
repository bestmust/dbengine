#ifndef DEFS_H
#define DEFS_H
#define MANUALTEST 0
#define MANUALDEBUG 1
#if MANUALDEBUG
#define MDLog(x,y)  ( cout<<"\n"<<x<<" - "<<y )
#else
#define MDLog(x,y) ( cout<<"" )
#endif

#define MAX_ANDS 20
#define MAX_ORS 20
#define PIPE_BUFF_SIZE 200
#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();

typedef enum {
    heap, sorted, tree
} fType;

typedef enum {
    reading, writing
} Mode;

// Structure to store the meta data of DBFile
typedef struct MetaInfo {
    // Type of the file {heap, sorted, tree}
    fType fileType;
} Meta;




#endif

