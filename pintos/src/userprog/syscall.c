#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "filesys/filesys.h"
#include "filesys/file.h"
#include "threads/vaddr.h"
#include "threads/malloc.h"
#include "userprog/pagedir.h"
#include "process.h"
#include "devices/shutdown.h"

static void syscall_handler (struct intr_frame *);
static struct list file_directory;
static int fd_i;

struct file_elem{
  int fd;
  struct file * file;
  //tid_t tid;
  struct list_elem elem;
};

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
  list_init(&file_directory);
  fd_i = 2;
}

static int 
syscall_null (int i)
{
	return (i + 1);
}

static int    
syscall_write (int fd, const void *buffer, unsigned size){ 
  if (fd == 1){
    putbuf(buffer, size);
    return size;
  }
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == (int) fd){
      //if (fe->tid != thread_current()->tid) return -1;
      return file_write(fe->file, buffer, size);
    }
  }
  return -1;
}

static void 
syscall_close(int fd){
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == fd){
      //if (fe->tid != thread_current()->tid) return;
      file_close(fe->file);
      list_remove(e);
      free(fe);
      return;
    }
  }
}

int
syscall_exit(int status){
  printf("%s: exit(%d)\n", thread_current()->name, status);
  struct thread *cur = thread_current ();
  struct list_elem *e;
  for (e = list_begin (&cur->children); e != list_end (&cur->children); e = list_next (e)){
    struct child_list_elem *c = list_entry (e, struct child_list_elem, child_elem);
    get_thread_by_tid(c->tid)->parent = NULL;
  }
  if (cur->parent){
    for (e = list_begin (&cur->parent->children); e != list_end (&cur->parent->children); e = list_next (e)){
      struct child_list_elem *c = list_entry (e, struct child_list_elem, child_elem);
      if (c->tid == cur->tid){
        c->exited = 1;
        c->exit_status = status;
        break;
      }
    }
    if (cur->parent->waitingOn == cur->tid){
      sema_up(&cur->parent->sema);
    }
  }
  //for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
  //  struct file_elem *fe = list_entry (e, struct file_elem, elem);
  //  if (fe->tid == cur->tid) syscall_close(fe->fd);
  //}
  file_close(cur->file);
  thread_exit();
  return status;
}

static int
syscall_open(const char* filename){
  struct file * fi = filesys_open(filename);
  if (!fi){
    return -1;
  }
  struct file_elem * fe = malloc(sizeof(struct file_elem));
  fe->fd = fd_i;
  //fe->tid = thread_current()->tid;
  fd_i++;
  fe->file = fi;
  list_push_front(&file_directory, &fe->elem);
  return fe->fd;
}

static int
syscall_filesize(int fd){
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == fd){
      //if (fe->tid != thread_current()->tid) return -1;
      return file_length(fe->file);
    }
  }
  return -1;
}

static int
syscall_read(int fd, void* buffer, unsigned size){
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == fd){
      //if (fe->tid != thread_current()->tid) return -1;
      return file_read(fe->file, buffer, size);
    }
  }
  return -1;
}

static void
syscall_seek(int fd, unsigned size){
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == fd){
      //if (fe->tid != thread_current()->tid) return;
      file_seek(fe->file, size);
      return;
    }
  }
}

static unsigned
syscall_tell(int fd){
  struct list_elem * e;
  for (e = list_begin (&file_directory); e != list_end (&file_directory); e = list_next (e)){
    struct file_elem *fe = list_entry (e, struct file_elem, elem);
    if (fe->fd == fd){
      //if (fe->tid != thread_current()->tid) syscall_exit(-1);
      return file_tell(fe->file);
    }
  }
  return -1;
}

static void
syscall_handler (struct intr_frame *f) 
{
  uint32_t* args = ((uint32_t*) f->esp);
  if(!is_user_vaddr((void*)args) || !pagedir_get_page(thread_current()->pagedir, (void*)args)) syscall_exit(-1);
  switch (args[0]) {
    case SYS_EXIT:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      f->eax = syscall_exit(args[1]);
      break;
    case SYS_NULL:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      f->eax = syscall_null(args[1]);
      break;
    case SYS_WAIT:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      f->eax = process_wait((tid_t) args[1]);
      break;
    case SYS_EXEC:
      if(!is_user_vaddr((void*)args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[1])) syscall_exit(-1);
      f->eax = process_execute((char*) args[1]);
      break;
    case SYS_HALT:
      shutdown_power_off();
    case SYS_CREATE:
      if(!args[1] || !is_user_vaddr((void*)args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[1])) syscall_exit(-1);
      f->eax = filesys_create((const char*)args[1], (unsigned)args[2]);
      break;
    case SYS_REMOVE:
      if(!args[1] || !is_user_vaddr((void*)args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[1])) syscall_exit(-1);
      f->eax = filesys_remove((const char*)args[1]);
      break;
    case SYS_OPEN:
      if(!args[1] || !is_user_vaddr((void*)args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[1])) syscall_exit(-1);
      f->eax = syscall_open((const char*)args[1]);
      break;
    case SYS_FILESIZE:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      f->eax = syscall_filesize((int)args[1]);
      break;
    case SYS_READ:
      if(!args[2] || !is_user_vaddr((void*)args[2]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[2])) syscall_exit(-1);
      f->eax = syscall_read((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;
    case SYS_WRITE:
      if(!args[2] || !is_user_vaddr((void*)args[2]) || !pagedir_get_page(thread_current()->pagedir, (void*)args[2])) syscall_exit(-1);
      f->eax = syscall_write((int)args[1], (void*) args[2], (unsigned)args[3]);
      break;
    case SYS_SEEK:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      syscall_seek((int)args[1], (unsigned)args[2]);
      break;
    case SYS_TELL:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      f->eax = syscall_tell((int)args[1]);
      break;
    case SYS_CLOSE:
      if(!is_user_vaddr((void*)&args[1]) || !pagedir_get_page(thread_current()->pagedir, (void*)&args[1])) syscall_exit(-1);
      syscall_close((int)args[1]);
  }
}
