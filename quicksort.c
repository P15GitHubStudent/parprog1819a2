#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#define QUEUE_SIZE 100000
#define THREADS 4


#define SHUTDOWN 0
#define SORT 1
#define SORTED 2

#define N 500000
//#define N 1000
//#define LIMIT 250000
#define THRESHOLD 1000

#define DEBUG true

// H domh ton munimaton pou xrhsimopoietai gia thn oura ton munimaton
struct message {
    int type;
    double * arr;
    int low;
    int high;
};

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;

struct message mqueue[QUEUE_SIZE];
int q_in = 0, q_out = 0; // deiktes gia thn eisagogi, eksagogi stoixeion
int m_count = 0; // posa stoixeia exei h oura


void swap(double *a, double *b) {
    double tmp = *a;
    *a = *b;
    *b = tmp;
} 

// Sunartisi xrhsimh gia aplo debugging tou kodika!
const char * command_str(int command){
    if (command == SHUTDOWN){
        return "SHUTDOWN";
    }
    else if(command == SORTED){
        return "SORTED";
    }
    else if(command == SORT){
        return "SORT";
    }
    else{
        return "UNKNOWN COMMAND!";
    }
}


int partition(double *a, int n) {
    int first = 0;
    int middle = n/2;
    int last = n-1;
    if (a[first] > a[middle]) {
        swap(a+first, a+middle);
    }
    if (a[middle] > a[last]) {
        swap(a+middle, a+last);
    }
    if (a[first] > a[middle]) {
        swap(a+first, a+middle);
    }
    double p = a[middle];
    int i, j;
    for (i=1, j=n-2;; i++, j--) {
        while (a[i] < p) i++;
        while (a[j] > p) j--;
        if (i>=j) break;
        swap(a+i, a+j);
    }
    return i;
}



void ins_sort(double *a, int n) {
    int i, j;
    for (i=1; i<n; i++) {
        j = i;
        while (j>0 && a[j-1] > a[j]) {
            swap(a+j, a+j-1);
            j--;    
        }
    }
}

void send(int type, int low, int high) {

    //Arxi krisimou tmimatos se auto to shmeio ama to mutex den to exei hdh kapoia synartisi tha lockarei eidallos tha synexisei
    pthread_mutex_lock(&mutex);
    
    // gia na apofugoume spurious wakeup! 
    while (m_count >= QUEUE_SIZE){
        printf("\n send locked\n");
        // topothetei to thread se Sleep mode oso isxuei h sunthiki sto while loop kai apeleuthorinei to mutex
        // gia na mporei na ksanaxrhsimopoithei
        pthread_cond_wait(&msg_out, &mutex); 
    }
    
    // Eisagogi ton dedomenon stin thesi pou upodilonetai apo ton akeraio deikti q_in
    mqueue[q_in].type = type;
    //mqueue[q_in].arr = arr;
    mqueue[q_in].low = low;
    mqueue[q_in].high = high;

    q_in++; // Efoson exei ginei eisagoi tou stoixeiou deixnoume sto epomeno

    // reset  tis kuklikis ouras
    if (q_in == QUEUE_SIZE) {
        q_in = 0;
    }

    m_count++; // metritis gia ta sunolika stoixeia pou exoun eisaxthei stin oura
    //efoson exoume stoixeia mporoume na ksipnisoume ton receiver
    // kai efoson den xreiazomaste na peiraksoume to krisimo tmima kanoume unlock!
    pthread_cond_signal(&msg_in); //
    pthread_mutex_unlock(&mutex);
    // pthread_exi(NULL);
}

//Me authn thn sunartisi stelnoume mono ton typo p.x SHUTDOWN munima theloume na steiloume mono ton typo
void sendType(int type){
    send(type,0,0);
}

//mia enalaktiki idea xoris na bgazo paketa apo tin oura kanontas ta pragmata pio grigora
void current_queue_type(int * type){
    pthread_mutex_lock(&mutex);
    while(m_count < 1){
        pthread_cond_wait(&msg_in, &mutex);
    }
    *type = mqueue[q_out].type;
    pthread_cond_signal(&msg_out);
    pthread_mutex_unlock(&mutex);
}

// painrei ton trexon stoixeio apo thn oura kai ta topothetei stis metablites pou briskontai stiw parametrous
void recv(int *type, int * low, int * high){
    //Krhsimo Tmhma
    pthread_mutex_lock(&mutex);

    //Efoson sthn oura den uparxoun stoixeia topothetise to thread se sleep mode kai apeleutherose to mutex gia na mporei na 
    // ginei xrhsh tou !
    while (m_count < 1){
        printf("\n recv locked\n");
        pthread_cond_wait(&msg_in, &mutex);
    }

    //pairnoume tis trexon plirofories
    *type = mqueue[q_out].type;
    *low = mqueue[q_out].low;
    *high = mqueue[q_out].high;

    q_out++; // deixnoume sto epomeno stoixeio gia eksagogi

    if (q_out == QUEUE_SIZE) { // epanafora tis ouras
        q_out = 0;
    }

    m_count--; // efoson phrame stoixeia

    pthread_cond_signal(&msg_out);
    pthread_mutex_unlock(&mutex); // telos krhsimimou tmhmatos
}

void sortFunc(double * arr, int low, int high){

    // Se periptosi pou ta stoixeia einai mikrotera apo to THRESHOLD kane taksinomhsh me thn InsertionSort
    if (high - low <= THRESHOLD){
        ins_sort(arr + low, high - low);
        send(SORTED, low, high);
        return;
        //printf(" Sorted Arr with startPos:%d and endPos:%d\n", startPos, endPos);
        //pthread_exit(NULL);
    }

    //printf("2st array element: %f \n", arr[0]);

    // printf("\n-------------------------------------------\n");

    // printf("low:%d high:%d \n", low, high);

    int partIndex = partition(arr + low, high - low);

    // printf("PartIndex:%d", partIndex);

    // printf("\n ---------------------------------- \n");

    send(SORT, low, low + partIndex); 

    send(SORT, low + partIndex, high);


}

void *thread_func(void *params) {
    int messageType, low, high;
    double * arr = (double *)params;

    recv(&messageType, &low, &high);

    // Efoson den exei stalthei to minima tou shutdown!
    while (messageType != SHUTDOWN){

         printf("Received MsgType:%s startPos:%d, endPos:%d \n", command_str(messageType), low, high);

        // Pairnei sunexeia munimata apo thn oura kai se periprosi pou einai munima taksinomisis analambanei na to taksinomhsh
        if (messageType == SORT){
            sortFunc(arr, low, high);
        }
        // Se periptosi pou ta munimata einai sorted ta ksanaeisagoume sthn oura
         else if(messageType == SORTED){
            //printf("SORTED: startPos:%d endPos:%d\n", startPos, endPos);
            send(SORTED, low, high);
            //sendType(SHUTDOWN);
         }
         else if(messageType == SHUTDOWN){
            sendType(SHUTDOWN);
         }
        
        //Pairnoume to epomeno munhma apo thn oura
        recv(&messageType, &low, &high);
        
    }

    // Molis bgei enhmeronei kai ta alla threads oti termatistike etsi oste gia na termatisoun kai auta
    sendType(SHUTDOWN);
    printf("done!\n");
    pthread_exit(NULL);
}

int main(int argc, char ** argv){

     double * arr;
    //Desmeusi tou pinaka sto heap
    arr = (double*) malloc(sizeof(double) * N);

    // Se periptosi pou h mnimi den desmeutike me epituxia!
    if (arr == NULL) {
        printf("Error during memory allocation\n");
        exit(1);
    }

    // Arxikopoihsh pinakon
    for (int i=0; i<N; i++){
        arr[i] = (double) rand() / RAND_MAX;
    }

    //Dhmiorgia ton thread pools kathe thread tha parei os parametro ton pinaka ton opoio tha prospathisoume na kanoume sort
    pthread_t threads[THREADS];
    for (int i=0; i<THREADS; i++){
        //int thread_id = i;
        if (pthread_create(&threads[i], NULL, thread_func, arr) != 0){
            printf("Thread creation error\n");
            free(arr);
            exit(1);
        }
    }



    //To proto munima pou tha stalthei einai na ginei taksinomhsh tou pinaka
    send(SORT, 0, N);

    int messageType, low, high;
    int count = 0;

    //lambanei to proto munima apo thn oura
    recv(&messageType, &low, &high);

    while (1){
        //taksinomish stoixeion den analambanetai apo thn main gia auto ksanastelnetai, to idio isxuei gia to munima SHUTDOWN
        if (messageType == SORT){
            send(SORT, low, high);
        }

        else if (messageType == SHUTDOWN){
            sendType(SHUTDOWN);
        }
        // Taksinomithkan stoixeia
        else if (messageType == SORTED){
            int itemsSorted = high - low;
            count += itemsSorted;
            printf("Sorted values with %d / %d\n", count, N);
            if (count >= N){
                printf("FINISHED");
                break;
            }
        }
        //lambanoume to epomeno munima apo thn oura
        recv(&messageType, &low, &high);
        //printf(" COMMAND TO RUN: %s startPos: %d endPos: %d mcount: % d \n", command_str(messageType), startPos, endPos, m_count);
    }

      // se auto to shmeio ola ta stoixeia tou pinaka taksinomithikan kai zhtame apok ta threads na termatistoun
     sendType(SHUTDOWN);

    // perimenoume na kleisoun ola ta threads proto sunexisoume
    for (int i=0; i<THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // gia na sigoureutoume oti to teliko array pou bgike egine sort me epituxia
    int i;
    for (i=0; i<N-1; i++) {
        if (arr[i] > arr[i+1]) {
            printf("Error! Array is not sorted. a[%d] = %lf, a[%d] = %lf\n", i, arr[i], i+1, arr[i+1]);
            break;
        }
    }

    // success message
    if (i >= N-1){
        printf("ARRAY IS SORTED!\n");
    }


    // apodesmeusi poron
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&msg_in);
    pthread_cond_destroy(&msg_out);
    free(a);
    //pthread_mutex_destroy(&debugMutex);

    return 0;
}
