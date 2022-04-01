//! @file evlo.c

#include "vere/evlo.h"

#include "c/bile.h"
#include "noun/events.h"
#include "noun/manage.h"
#include "vere/epoc.h"

//==============================================================================
// Constants
//==============================================================================

//! Name of file containing the fake bit.
static const c3_c const fak_nam_c[] = "fake.bin";

//! Name of file containing the lifecycle length.
static const c3_c const lif_nam_c[] = "lifecycle.bin";

//! Name of file containing event log version.
static const c3_c const ver_nam_c[] = "version.bin";

//! Name of file containing the name of the ship.
static const c3_c const who_nam_c[] = "who.bin";

//! Event log version number.
static const c3_w elo_ver_w = 1;

//! Max number of events per epoch.
static const size_t epo_len_i = 100;

//==============================================================================
// Static functions
//==============================================================================

static inline c3_t
_is_epoc_dir(const c3_c* const nam_c)
{
  return 0 == strncmp(nam_c, epo_pre_c, strlen(epo_pre_c));
}

static inline c3_i
_cmp_epocs(const void* lef_v, const void* rih_v)
{
  static const size_t siz_i = sizeof(((struct dirent*)NULL)->d_name);
  const c3_c*         lef_c = *(const c3_c(*)[siz_i])lef_v;
  const c3_c*         rih_c = *(const c3_c(*)[siz_i])rih_v;
  const c3_i          dif_i = strlen(lef_c) - strlen(rih_c);
  return 0 == dif_i ? strcmp(lef_c, rih_c) : dif_i;
}

//! Discover epoch directories in a given directory.
//!
//! @param[in]  dir_c  Directory to search for epoch directories.
//! @param[out] ent_c  Pointer to array of 256-byte arrays.
//! @param[out] ent_i  Pointer to number of elements in `*ent_c`.
//!
//! @return 1  Discovered one or more epoch directories.
//! @return 0  Otherwise.
//!
//! @n (1) Arbitrarily choose 16 as the initial guess at the max number of
//!        epochs.
static c3_t
_read_epoc_dirs(const c3_c* const dir_c, c3_c (**ent_c)[], size_t* ent_i)
{
  *ent_c = NULL;
  *ent_i = 0;

  DIR* dir_u;
  if ( !dir_c || !ent_c || !ent_i || !(dir_u = opendir(dir_c)) ) {
    return 0;
  }

  struct dirent* ent_u;
  const size_t   siz_i = sizeof(ent_u->d_name);
  size_t         cap_i = 16;
  c3_c(*dst_c)[siz_i]  = c3_malloc(cap_i * siz_i);
  size_t dst_i         = 0;
  while ( ent_u = readdir(dir_u) ) {
    if ( !_is_epoc_dir(ent_u->d_name) ) {
      continue;
    }
    if ( dst_i == cap_i ) {
      cap_i *= 2;
      dst_c = c3_realloc(dst_c, cap_i * siz_i);
    }
    strcpy(dst_c[dst_i++], ent_u->d_name);
  }
  if ( 0 == dst_i ) {
    c3_free(dst_c);
    return 0;
  }
  qsort(dst_c, dst_i, siz_i, _cmp_epocs);
  *ent_c = dst_c;
  *ent_i = dst_i;
  return 1;
}

static inline void
_remove_committed_events(u3_evlo* const log_u)
{
  c3_list* eve_u = log_u->eve_u.lis_u;
  size_t   len_i = log_u->eve_u.req_i;
  for ( size_t idx_i = 0; idx_i < len_i; idx_i++ ) {
    c3_free(c3_list_popf(eve_u));
  }
}

//! Determine if an epoch is full.
static inline c3_t
_is_full(const u3_epoc* const poc_u)
{
  return !u3_epoc_is_empty(poc_u)
         && 0 == u3_epoc_last_commit(poc_u) % epo_len_i;
}

static inline u3_epoc*
_rollover(u3_evlo* const log_u)
{
  return log_u->epo_u.cur_u = c3_list_data(c3_list_peekb(log_u->epo_u.lis_u));
}

//! Get the number of events that can be committed to an epoch.
static inline size_t
_request_len(const u3_epoc* const poc_u, const c3_list* const eve_u)
{
  size_t len_i = u3_epoc_len(poc_u);
  c3_assert(len_i <= epo_len_i);
  size_t rem_i = epo_len_i - len_i;
  return c3_min(rem_i, c3_list_len(eve_u));
}

//! Invoke user callback after batch async commit. On main thread.
//!
//! @n (1) Attempt to commit events that were enqueued after the commit began.
static void
_uv_commit_after_cb(uv_work_t* req_u, c3_i sas_i)
{
  u3_evlo* log_u = req_u->data;
  log_u->act_t   = 0;

  c3_t suc_t = log_u->asy_u.suc_t;
  if ( suc_t ) {
    _remove_committed_events(log_u);
  }

  c3_d las_d = u3_epoc_last_commit(log_u->epo_u.cur_u);
  log_u->asy_u.com_f(log_u->asy_u.ptr_v, las_d, suc_t);

  if ( UV_ECANCELED != sas_i ) { // (1)
    u3_evlo_commit(log_u, NULL, 0);
  }
}

//! Kick off async batch commit. Off main thread.
static void
_uv_commit_cb(uv_work_t* req_u)
{
  u3_evlo*      log_u = req_u->data;
  u3_epoc*      poc_u = log_u->epo_u.cur_u;
  c3_list_node* nod_u = c3_list_peekf(log_u->eve_u.lis_u);
  size_t        len_i = log_u->eve_u.req_i;
  log_u->asy_u.suc_t  = u3_epoc_commit(poc_u, nod_u, len_i);
}

//! Find the epoch of the event log that the given event ID is a part of.
static u3_epoc*
_find_epoc(u3_evlo* const log_u, const c3_d ide_d)
{
  c3_list_node* nod_u = c3_list_peekb(log_u->epo_u.lis_u);
  u3_epoc*      poc_u;
  while ( nod_u ) {
    poc_u = c3_list_data(nod_u);
    if ( u3_epoc_has(poc_u, ide_d) ) {
      break;
    }
    nod_u = nod_u->pre_u;
  }
  return nod_u ? poc_u : NULL;
}

static inline c3_t
_is_async(const u3_evlo* const log_u)
{
  return u3_evlo_async == log_u->mod_e;
}

static inline c3_t
_has_active_commit(const u3_evlo* const log_u)
{
  return log_u->act_t;
}

//==============================================================================
// Functions
//==============================================================================

//! @n (1) Persist metadata.
//!        TODO(peter): fix portability issues between machines of different
//!                     endianness.
//!        TODO(peter): handle event log version number
//!        TODO(peter): move lifecycle file to first epoch
//! @n (2) Create first epoch.
u3_evlo*
u3_evlo_new(const c3_path* const pax_u, const u3_meta* const met_u)
{
  u3_evlo* log_u = c3_calloc(sizeof(*log_u));
  if ( !(log_u->pax_u = c3_path_fv(1, pax_u->str_c)) ) {
    goto free_event_log;
  }
  mkdir(log_u->pax_u->str_c, 0700);

  { // (1)
    const void* dat_v;

    c3_path_push(log_u->pax_u, fak_nam_c);
    dat_v = &met_u->fak_o;
    if ( !c3_bile_write_new(log_u->pax_u, dat_v, sizeof(met_u->fak_o)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);

    c3_path_push(log_u->pax_u, lif_nam_c);
    dat_v = &met_u->lif_w;
    if ( !c3_bile_write_new(log_u->pax_u, dat_v, sizeof(met_u->lif_w)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);

    c3_path_push(log_u->pax_u, who_nam_c);
    dat_v = &met_u->who_d;
    if ( !c3_bile_write_new(log_u->pax_u, dat_v, sizeof(met_u->who_d)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);
  }

  { // (2)
    try_list(log_u->epo_u.lis_u = c3_list_init(), goto free_event_log);
    u3_epoc* poc_u;
    try_epoc(poc_u = u3_epoc_new(log_u->pax_u, epo_min_d), goto free_event_log);
    c3_list_pushb(log_u->epo_u.lis_u, poc_u, sizeof(*poc_u));
    c3_free(poc_u);
    log_u->epo_u.cur_u = c3_list_data(c3_list_peekb(log_u->epo_u.lis_u));
  }

  try_list(log_u->eve_u.lis_u = c3_list_init(), goto free_event_log);

  goto succeed;

free_event_log:
  u3_evlo_close(log_u);
  c3_free(log_u);
  return NULL;

succeed:
  return log_u;
}

//! @n (1) Read metadata from filesystem.
u3_evlo*
u3_evlo_open(const c3_path* const pax_u, u3_meta* const met_u)
{
  u3_evlo* log_u = c3_calloc(sizeof(*log_u));
  if ( !(log_u->pax_u = c3_path_fv(1, pax_u->str_c)) ) {
    goto free_event_log;
  }

  { // (1)
    void* dat_v;

    c3_path_push(log_u->pax_u, fak_nam_c);
    dat_v = &met_u->fak_o;
    if ( !c3_bile_read_existing(log_u->pax_u, dat_v, sizeof(met_u->fak_o)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);

    c3_path_push(log_u->pax_u, lif_nam_c);
    dat_v = &met_u->lif_w;
    if ( !c3_bile_read_existing(log_u->pax_u, dat_v, sizeof(met_u->lif_w)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);

    c3_path_push(log_u->pax_u, who_nam_c);
    dat_v = &met_u->who_d;
    if ( !c3_bile_read_existing(log_u->pax_u, dat_v, sizeof(met_u->who_d)) ) {
      goto free_event_log;
    }
    c3_path_pop(log_u->pax_u);
  }

  c3_c(*ent_c)[sizeof(((struct dirent*)NULL)->d_name)];
  size_t ent_i;
  if ( !_read_epoc_dirs(log_u->pax_u->str_c, &ent_c, &ent_i) ) {
    goto free_event_log;
  }

  try_list(log_u->epo_u.lis_u = c3_list_init(), goto free_dir_entries);
  for ( size_t idx_i = 0; idx_i < ent_i; idx_i++ ) {
    c3_path_push(log_u->pax_u, ent_c[idx_i]);
    u3_epoc* poc_u;
    try_epoc(poc_u = u3_epoc_open(log_u->pax_u), goto free_dir_entries);
    c3_list_pushb(log_u->epo_u.lis_u, poc_u, sizeof(*poc_u));
    c3_free(poc_u);
    c3_path_pop(log_u->pax_u);
  }
  log_u->epo_u.cur_u = c3_list_data(c3_list_peekb(log_u->epo_u.lis_u));
  log_u->eve_d = u3_epoc_last_commit(log_u->epo_u.cur_u);

  try_list(log_u->eve_u.lis_u = c3_list_init(), goto free_dir_entries);

  c3_free(ent_c);
  goto succeed;

free_dir_entries:
  c3_free(ent_c);
free_event_log:
  u3_evlo_close(log_u);
  c3_free(log_u);
  return NULL;

succeed:
  return log_u;
}

void
u3_evlo_commit_mode(u3_evlo* const log_u, u3_evlo_acon* asy_u)
{
  // TODO(peter): wait for async commits to complete?

  if ( !asy_u ) {
    log_u->mod_e = u3_evlo_sync;
    return;
  }

  log_u->mod_e = u3_evlo_async;
  log_u->asy_u = (u3_evlo_acon){
    .lup_u      = asy_u->lup_u,
    .req_u.data = log_u,
    .com_f      = asy_u->com_f,
    .ptr_v      = asy_u->ptr_v,
  };
}

//! @n (1) A NULL event can be passed to invoke another commit batch.
//! @n (2) Timing of rollover is key: TODO(peter).
c3_t
u3_evlo_commit(u3_evlo* const log_u, c3_y* const byt_y, const size_t len_i)
{
  c3_list* eve_u = log_u->eve_u.lis_u;
  if ( byt_y ) { // (1)
    c3_list_pushb(eve_u, byt_y, len_i);
    log_u->eve_d++;
  }
  else if ( 0 == c3_list_len(eve_u) ) {
    goto succeed;
  }

  c3_list* epo_u = log_u->epo_u.lis_u;
  u3_epoc* poc_u = log_u->epo_u.cur_u;
  if ( 0 == log_u->eve_d % epo_len_i ) {
    u3_epoc* new_u = u3_epoc_new(log_u->pax_u, log_u->eve_d + 1);
    c3_assert(new_u);
    c3_list_pushb(epo_u, new_u, sizeof(*new_u));
    c3_free(new_u);
  }

  switch ( log_u->mod_e ) {
    case u3_evlo_sync:
      if ( _is_full(poc_u) ) {
        poc_u = _rollover(log_u);
      }
      log_u->eve_u.req_i  = _request_len(poc_u, eve_u);
      c3_list_node* nod_u = c3_list_peekf(eve_u);
      log_u->act_t        = 1;
      if ( !u3_epoc_commit(poc_u, nod_u, log_u->eve_u.req_i) ) {
        goto fail;
      }
      _remove_committed_events(log_u);
      log_u->act_t = 0;
      goto succeed;
    case u3_evlo_async:
      if ( !_has_active_commit(log_u) ) {
        if ( _is_full(poc_u) ) {
          poc_u = _rollover(log_u);
        }
        log_u->eve_u.req_i = _request_len(poc_u, eve_u);
        log_u->act_t       = 1;
        uv_queue_work(log_u->asy_u.lup_u,
                      &log_u->asy_u.req_u,
                      _uv_commit_cb,
                      _uv_commit_after_cb);
      }
      goto succeed;
  }

fail:
  return 0;

succeed:
  return 1;
}

c3_t
u3_evlo_replay(u3_evlo* const log_u,
               c3_d           cur_d,
               c3_d           las_d,
               u3_evlo_play   pla_f,
               void*          ptr_v)
{
// Replay using most recent epoch's snapshot.
#if 1
  c3_t suc_t = 0;

  if ( 0 == las_d ) {
    las_d = u3_epoc_last_commit(log_u->epo_u.cur_u);
  }

  if ( las_d == cur_d ) {
    suc_t = 1;
    goto end;
  }

  u3_epoc* poc_u = _find_epoc(log_u, las_d);

  // TODO(peter): ensure that epoch's snapshot > inc snapshot
  // TODO(peter): take close look at ref counts wrt replay
  if ( !u3_epoc_is_first(poc_u) ) {
    cur_d = u3_epoc_first_commit(poc_u) <= u3A->eve_d
            ? u3A->eve_d
            : u3m_boot(u3_epoc_path(poc_u), u3e_load);
  }
  cur_d++;

  try_epoc(u3_epoc_iter_open(poc_u, cur_d), goto take_snapshot);
  while ( cur_d <= las_d ) {
    c3_y*  byt_y;
    size_t len_i;
    try_epoc(u3_epoc_iter_step(poc_u, &byt_y, &len_i), goto close_iterator);
    if ( !pla_f(ptr_v, cur_d, las_d, byt_y, len_i) ) {
      goto close_iterator;
    }
    cur_d++;
  }
  suc_t = 1;

close_iterator:
  u3_epoc_iter_close(poc_u);
take_snapshot:
  u3e_save();
end:
  return suc_t;
// Replay without snapshot.
#else
  if ( 0 == las_d ) {
    las_d = u3_epoc_last_commit(log_u->epo_u.cur_u);
  }

  if ( las_d == cur_d ) {
    return 1;
  }

  cur_d++;

  c3_list_node* nod_u = c3_list_peekf(log_u->epo_u.lis_u);
  u3_epoc*      poc_u = NULL;
  c3_d          end_d = 0;
  while ( 1 ) {
    if ( !poc_u ) {
      poc_u = c3_list_data(nod_u);
      end_d = u3_epoc_last_commit(poc_u);
      try_epoc(u3_epoc_iter_open(poc_u, cur_d), exit(9));
    }
    c3_y*  byt_y;
    size_t len_i;
    try_epoc(u3_epoc_iter_step(poc_u, &byt_y, &len_i), exit(10));
    if ( !pla_f(ptr_v, cur_d, las_d, byt_y, len_i) ) {
      exit(11);
    }
    cur_d++;
    if ( las_d < cur_d ) {
      u3_epoc_iter_close(poc_u);
      break;
    }
    else if ( end_d < cur_d ) {
      u3_epoc_iter_close(poc_u);
      poc_u = NULL;
      nod_u = nod_u->nex_u;
    }
  }
  return 1;
#endif
}

//! @n (1) Cancel thread that is performing async commits.
//!        XX can deadlock from signal handler
//!        XX revise SIGTSTP handling
//! @n (2) Free epochs.
//! @n (3) Free events pending commit.
void
u3_evlo_close(u3_evlo* const log_u)
{
  if ( !log_u ) {
    return;
  }

  if ( _is_async(log_u) && _has_active_commit(log_u) ) { // (1)
      while ( UV_EBUSY == uv_cancel((uv_req_t*)&log_u->asy_u.req_u) );
  }

  if ( log_u->pax_u ) {
    c3_free(log_u->pax_u);
  }

  { // (2)
    c3_list* epo_u = log_u->epo_u.lis_u;
    if ( epo_u ) {
      c3_list_node* nod_u;
      while ( nod_u = c3_list_popf(epo_u) ) {
        u3_epoc* poc_u = c3_list_data(nod_u);
        u3_epoc_close(poc_u);
        c3_free(nod_u);
      }
      c3_free(epo_u);
    }
  }

  { // (3)
    c3_list* eve_u = log_u->eve_u.lis_u;
    if ( eve_u ) {
      for ( size_t idx_i = 0; idx_i < c3_list_len(eve_u); idx_i++ ) {
        c3_free(c3_list_popf(eve_u));
      }
      c3_free(eve_u);
    }
  }
}
