//! @file epoc.c

#include "vere/epoc.h"

#include <sys/stat.h>

#include "all.h"
#include "vere/vere.h"

//==============================================================================
// Constants
//==============================================================================

const c3_c epo_pre_c[] = "0i";

const c3_d epo_min_d = 1ULL;

//==============================================================================
// Macros
//==============================================================================

//! Error-handling wrapper for LMDB API calls that return 0 on success and
//! non-zero on failure.
//!
//! @param[in] lmdb_call       LMDB API call.
//! @param[in] failure_action  Statement to execute after logging failure.
#define try_lmdb(lmdb_call, failure_action, ...)                               \
  do {                                                                         \
    c3_i ret_i = lmdb_call;                                                    \
    if ( 0 != ret_i ) {                                                        \
      fprintf(stderr, "lmdb: " __VA_ARGS__);                                   \
      fprintf(stderr, " (%s)\r\n", mdb_strerror(ret_i));                       \
      failure_action;                                                          \
    }                                                                          \
  } while ( 0 )

//==============================================================================
// Static functions
//==============================================================================
// Note that the `static` keyword is not used so that tests can surface
// these functions by declaring the function prototype in the test file.

//! Build an epoch path of the form `<par_c>/0i<fir_d>`.
//!
//! @param[in] par_u  Path to parent directory.
//! @param[in] fir_d
//!
//! @n (1) This is the largest possible unsigned 64-bit number.
c3_path*
_epoc_path(const c3_path* const par_u, const c3_d fir_d)
{
  static const c3_c const lar_c[] = "18446744073709551615"; // (1)

  c3_c dir_c[sizeof(epo_pre_c) + sizeof(lar_c)];
  snprintf(dir_c, sizeof(dir_c), "%s%" PRIu64, epo_pre_c, fir_d);

  return c3_path_fv(2, par_u->str_c, dir_c);
}

//! Open an epoch LMDB environment.
//!
//! An epoch's LMDB environment consists of only a single database--the default,
//! unnamed database--which is used to store the epoch's events.
//!
//! @param[in] pax_u  Path to directory containing LMDB environment.
//!
//! @n (1) From the LMDB docs: "The [map size] value should be chosen as large
//!        as possible, to accommodate future growth of the database."
MDB_env*
_lmdb_init(const c3_path* const pax_u)
{
  c3_i     ret_i;
  MDB_env* env_u;
  try_lmdb(mdb_env_create(&env_u), goto fail, "failed to create environment");

  // (1)
  static size_t siz_i =
#if ( defined(U3_CPU_aarch64) && defined(U3_OS_linux) ) || defined(U3_OS_mingw)
    0xf00000000;
#else
    0x10000000000;
#endif
  try_lmdb(mdb_env_set_mapsize(env_u, siz_i),
           goto close_env,
           "failed to set map size");

  try_lmdb(mdb_env_open(env_u, pax_u->str_c, 0, 0664),
           goto close_env,
           "failed to open environment at %s",
           pax_u->str_c);

  goto succeed;

close_env:
  mdb_env_close(env_u);
fail:
  return NULL;

succeed:
  return env_u;
}

//==============================================================================
// Functions
//==============================================================================

//! @n (1) Zero is not a valid value of `fir_d` because the `las_d` field of
//!        the `u3_epoc` handle is set to `fir_d - 1`, which would underflow if
//!        `0 == fir_d`.
//! @n (2) Take snapshot to save the state before the first event in the
//!        epoch is applied unless this is the very first epoch.
u3_epoc*
u3_epoc_new(const c3_path* const par_u, const c3_d fir_d)
{
  if ( !par_u || 0 == fir_d ) { // (1)
    goto fail;
  }

  u3_epoc* poc_u = c3_calloc(sizeof(*poc_u));

  poc_u->fir_d = fir_d;
  poc_u->las_d = fir_d - 1;
  poc_u->pax_u = _epoc_path(par_u, poc_u->fir_d);
  mkdir(poc_u->pax_u->str_c, 0700);

  if ( !(poc_u->env_u = _lmdb_init(poc_u->pax_u)) ) {
    goto free_epoch;
  }

  if ( epo_min_d != fir_d ) {
    u3e_save();
    if ( c3n == u3e_copy(poc_u->pax_u->str_c) ) { // (2)
      goto free_epoch;
    }
  }

  goto succeed;

free_epoch:
  u3_epoc_close(poc_u);
  c3_free(poc_u);
fail:
  return NULL;

succeed:
  return poc_u;
}

//! @n (1) TODO(peter): The first event "in" the epoch is the event at which the
//!        epoch's snapshot was taken, which is one before the first event that
//!        is in the epoch's LMDB instance.
u3_epoc*
u3_epoc_open(const c3_path* const pax_u)
{
  u3_epoc* poc_u = c3_calloc(sizeof(*poc_u));

  poc_u->pax_u = c3_path_fv(1, pax_u->str_c);

  if ( !(poc_u->env_u = _lmdb_init(poc_u->pax_u)) ) {
    goto free_epoc;
  }

  c3_i     ret_i;
  MDB_txn* txn_u;
  try_lmdb(mdb_txn_begin(poc_u->env_u, NULL, MDB_RDONLY, &txn_u),
           goto free_epoc,
           "failed to create read transaction");

  MDB_dbi dbi_u;
  try_lmdb(mdb_dbi_open(txn_u, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi_u),
           goto abort_txn,
           "failed to open default database");

  MDB_cursor* cur_u;
  try_lmdb(mdb_cursor_open(txn_u, dbi_u, &cur_u),
           goto abort_txn,
           "failed to open cursor");

  MDB_val key_u, val_u;
  try_lmdb(mdb_cursor_get(cur_u, &key_u, &val_u, MDB_FIRST),
           goto close_cursor,
           "failed to read first event number");
  poc_u->fir_d = *(c3_d*)key_u.mv_data;

  try_lmdb(mdb_cursor_get(cur_u, &key_u, &val_u, MDB_LAST),
           goto close_cursor,
           "failed to read last event number");
  poc_u->las_d = *(c3_d*)key_u.mv_data;

  goto succeed;

close_cursor:
  mdb_cursor_close(cur_u);
abort_txn:
  mdb_txn_abort(txn_u);
free_epoc:
  u3_epoc_close(poc_u);
  c3_free(poc_u);
fail:
  return NULL;

succeed:
  mdb_cursor_close(cur_u);
  mdb_txn_abort(txn_u); // (3)
  return poc_u;
}

c3_t
u3_epoc_commit(u3_epoc* const poc_u, c3_list_node* nod_u, const size_t len_i)
{
  MDB_txn* txn_u;
  try_lmdb(mdb_txn_begin(poc_u->env_u, NULL, 0, &txn_u),
           goto fail,
           "failed to create write transaction");

  MDB_dbi dbi_u;
  try_lmdb(mdb_dbi_open(txn_u, NULL, MDB_CREATE | MDB_INTEGERKEY, &dbi_u),
           goto abort_txn,
           "failed to open default database");

  c3_d ide_d = poc_u->las_d;
  for ( size_t idx_i = 0; idx_i < len_i; idx_i++ ) {
    ide_d++;
    if ( !nod_u ) {
      goto abort_txn;
    }
    MDB_val key_u = {.mv_data = &ide_d, .mv_size = sizeof(ide_d)};
    MDB_val val_u = {.mv_data = nod_u->dat_y, .mv_size = nod_u->len_i};

    try_lmdb(mdb_put(txn_u, dbi_u, &key_u, &val_u, MDB_NOOVERWRITE),
             goto abort_txn,
             "failed to write event %" PRIu64,
             ide_d);

    nod_u = nod_u->nex_u;
  }

  try_lmdb(mdb_txn_commit(txn_u),
           goto abort_txn,
           "failed to commit transaction");

  poc_u->las_d = ide_d;

  goto succeed;

abort_txn:
  mdb_txn_abort(txn_u);
fail:
  return 0;

succeed:
  return 1;
}

//! @n (1) Open read-only transaction.
//! @n (2) Open unnamed default database, which contains the epoch's events.
//! @n (3) Create cursor in events database.
//! @n (4) Position cursor on event ID `ide_d`.
c3_t
u3_epoc_iter_open(u3_epoc* const poc_u, c3_d ide_d)
{
  if ( poc_u->itr_u.ope_t ) {
    fprintf(stderr, "epoc: iterator already open\r\n");
    goto fail;
  }

  // (1)
  MDB_txn* txn_u = poc_u->itr_u.txn_u;
  try_lmdb(mdb_txn_begin(poc_u->env_u, NULL, MDB_RDONLY, &txn_u),
           goto fail,
           "failed to open read transaction");

  // (2)
  MDB_dbi dbi_u = poc_u->itr_u.dbi_u;
  try_lmdb(mdb_dbi_open(txn_u, NULL, MDB_INTEGERKEY, &dbi_u),
           goto abort_txn,
           "failed to open default database");

  // (3)
  MDB_cursor* cur_u = poc_u->itr_u.cur_u;
  try_lmdb(mdb_cursor_open(txn_u, dbi_u, &cur_u),
           goto abort_txn,
           "failed to open cursors");

  // (4)
  MDB_val key_u = {.mv_data = &ide_d, .mv_size = sizeof(ide_d)}, val_u;
  try_lmdb(mdb_cursor_get(cur_u, &key_u, &val_u, MDB_SET_KEY),
           goto close_cursor,
           "failed to position cursor on event ID %" PRIu64,
           ide_d);

  poc_u->itr_u = (u3_epoc_iter){
    .ope_t = 1,
    .txn_u = txn_u,
    .dbi_u = dbi_u,
    .cur_u = cur_u,
    .fir_d = ide_d,
    .cur_d = ide_d,
  };

  goto succeed;

close_cursor:
  mdb_cursor_close(cur_u);
abort_txn:
  mdb_txn_abort(txn_u);
fail:
  return 0;

succeed:
  return 1;
}

//! @n (1) We already consumed the last event in the epoch.
//! @n (2) No need to advance to the next key if this is the first call to
//!        u3_epoc_iter_step().
//! @n (3) The key we fetched from database didn't match the key we expected.
c3_t
u3_epoc_iter_step(u3_epoc* const poc_u, c3_y** const byt_y, size_t* const len_i)

{
  if ( !poc_u->itr_u.ope_t ) {
    fprintf(stderr, "epoc: no open iterator\r\n");
    goto fail;
  }

  if ( poc_u->las_d < poc_u->itr_u.cur_d ) { // (1)
    goto fail;
  }

  MDB_val       key_u, val_u;
  c3_t          fir_t = (poc_u->itr_u.fir_d == poc_u->itr_u.cur_d);
  MDB_cursor_op ops_u = (fir_t ? MDB_GET_CURRENT : MDB_NEXT); // (2)
  try_lmdb(mdb_cursor_get(poc_u->itr_u.cur_u, &key_u, &val_u, ops_u),
           goto fail,
           "failed to get event ID %" PRIu64,
           poc_u->itr_u.cur_d);

  c3_d key_d = *(c3_d*)key_u.mv_data;
  if ( key_d != poc_u->itr_u.cur_d ) { // (3)
    fprintf(stderr,
            "epoc: event ID mismatch: expected %" PRIu64 ", received %" PRIu64
            "\r\n",
            poc_u->itr_u.cur_d,
            key_d);
    goto fail;
  }

  *byt_y = val_u.mv_data;
  *len_i = val_u.mv_size;
  poc_u->itr_u.cur_d++;
  goto succeed;

fail:
  return 0;

succeed:
  return 1;
}

void
u3_epoc_iter_close(u3_epoc* const poc_u)
{
  mdb_cursor_close(poc_u->itr_u.cur_u);
  mdb_txn_abort(poc_u->itr_u.txn_u);
  poc_u->itr_u.ope_t = 0;
}

//! @n (1) Cancel thread that is performing async commits.
//!        XX can deadlock from signal handler
//!        XX revise SIGTSTP handling
void
u3_epoc_close(u3_epoc* const poc_u)
{
  if ( !poc_u ) {
    return;
  }

  if ( poc_u->env_u ) {
    mdb_env_close(poc_u->env_u);
  }
  if ( poc_u->pax_u ) {
    c3_free(poc_u->pax_u);
  }
}

#undef try_lmdb
