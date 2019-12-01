/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/

#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define QUEUE_SIZE                10
#define NUM_OF_THREADS             6

//Arxi ouras
int tail=0;
int head=0;
int count=0;

struct Queue
{
  int fd;
  struct timeval geo;
};

typedef struct Queue Queue;

Queue dim[QUEUE_SIZE];

void put(Queue osfp)
{
  if(count==QUEUE_SIZE)
  {
    printf("Queue is full,sorry!!!\n");
    exit(1);
  }
  count++;
  dim[tail]=osfp;
  tail++;
  tail=(tail%QUEUE_SIZE);
}

Queue get()
{
  if(count==0)
  {
    printf("Queue is empty,sorry!!!\n");
    exit(1);
  }
  Queue dim1;
  count--;
  dim1=dim[head];
  head++;
  head=head%QUEUE_SIZE;
  return dim1;
}
//telos ouras

int reader_count=0;
int writer_count=0;
int completed_request=0;
long total_waiting_time=0;
long total_service_time=0;
pthread_t m[NUM_OF_THREADS];

pthread_mutex_t for_queue = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t for_var = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t for_ReaderWriter = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t for_put = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t non_empty_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t non_full_queue = PTHREAD_COND_INITIALIZER;
pthread_cond_t writer = PTHREAD_COND_INITIALIZER;
pthread_cond_t reader = PTHREAD_COND_INITIALIZER;





// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:

          pthread_mutex_lock(&for_ReaderWriter);
          while(writer_count>0)
          {
            pthread_cond_wait(&writer,&for_ReaderWriter);
          }
          reader_count++;
          pthread_mutex_unlock(&for_ReaderWriter);
          // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);
            pthread_mutex_lock(&for_ReaderWriter);
            reader_count--;
            if(reader_count==0)
            {
              pthread_cond_signal(&reader);
            }
            pthread_mutex_unlock(&for_ReaderWriter);

            break;
          case PUT:
            pthread_mutex_lock(&for_ReaderWriter);
            while(reader_count!=0)
            {
              pthread_cond_wait(&reader,&for_ReaderWriter);
            }
            writer_count++;
            pthread_mutex_unlock(&for_ReaderWriter);
            // Write the given key/value pair to the database.
            pthread_mutex_lock(&for_put);
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
            pthread_mutex_unlock(&for_put);
            pthread_mutex_lock(&for_ReaderWriter);
            writer_count--;
            if(writer_count==0)
            {
              pthread_cond_signal(&writer);
            }
            pthread_mutex_unlock(&for_ReaderWriter);
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        close(socket_fd); //allios den douleuei
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}

void *consumers()
{
 struct timeval anamoni,support;

 while(1)
 {
  Queue dim3;
  pthread_mutex_lock(&for_queue);
  while(count==0)
  {
    pthread_cond_wait(&non_empty_queue,&for_queue);
  }
  dim3=get();
  pthread_cond_signal(&non_full_queue);
  pthread_mutex_unlock(&for_queue);

  gettimeofday(&anamoni,NULL);
  process_request(dim3.fd);
  gettimeofday(&support,NULL);
  pthread_mutex_lock(&for_var);
  total_waiting_time=total_waiting_time+((anamoni.tv_sec - dim3.geo.tv_sec) + (anamoni.tv_usec - dim3.geo.tv_usec));
  total_service_time=total_service_time+((support.tv_sec - anamoni.tv_sec) + (support.tv_usec - anamoni.tv_usec));
  completed_request++;
  pthread_mutex_unlock(&for_var);
 }

}


void sima(int s)
{
  long average1=total_waiting_time/(long)completed_request;
  long average2=total_service_time/(long)completed_request;

  KISSDB_close(db);
  if (db)
    free(db);
  db = NULL;

  printf("COMPLETED REQUEST: %d\n",completed_request);
  if(completed_request==0)
  {
    printf("Somethings went wrong!!!\n");
  }
  else
  {
    printf("AVERAGE WAITING TIME: %ld\n",average1);
    printf("AVERAGE SERVICE TIME: %ld\n",average2);
  }

  int k;
  for(k=0;k<NUM_OF_THREADS;k++)
  {
  	pthread_detach(m[k]);
  }

  exit(1);

}


/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  //dmiourgo ta nimata
  int i;
  for(i=0;i<NUM_OF_THREADS;i++)
  {
    pthread_create(&m[i],NULL,consumers,NULL);
  }

  int socket_fd,              // listen on this socket for new connections
      new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  //****signal(SIGPIPE,sima);*****/////
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  // main loop: wait for new connection/requests
  struct timeval tv;
  signal(SIGTSTP,sima);
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));
    Queue dim2;
    dim2.fd=new_fd;
    pthread_mutex_lock(&for_queue); //klidono nimata
    while(count>=QUEUE_SIZE)
    {
      pthread_cond_wait(&non_full_queue,&for_queue); //koimate
    }
    gettimeofday(&tv,NULL);
    dim2.geo=tv;
    put(dim2);
    pthread_cond_signal(&non_empty_queue); //ksipnaei
    pthread_mutex_unlock(&for_queue); //kselidono
  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

