#ifndef U3_VERE_MARS_H
#define U3_VERE_MARS_H

#include "c/portable.h"
#include "c/types.h"
#include "c/list.h"
#include "c/path.h"

#include <uv.h>

#include "vere/evlo.h"

//==============================================================================
// Types
//==============================================================================

//! Mars pending response.
typedef struct {
  c3_d  len_d; //!< length
  c3_y* hun_y; //!< bytes
  enum {
    u3_gift_fact_e = 0, //!< effects
    u3_gift_rest_e = 1, //!< any
  } sat_e;              //!< work state
  union {               //!< data
    c3_d  eve_d;        //!< event number
    void* ptr_v;        //!< any
  };
} u3_gift;

//! Handle for Urbit state machine.
typedef struct {
  c3_d     key_d[4]; //!< disk key
  c3_path* dir_u;    //!< execution directory (pier)
  c3_d     sen_d;    //!< last event requested
  c3_d     dun_d;    //!< last event processed
  c3_l     mug_l;    //!< hash of state
  c3_o     pac_o;    //!< pack kernel
  c3_o     rec_o;    //!< reclaim cache
  c3_o     mut_o;    //!< mutated kerne
  u3_noun  sac;      //!< space measurement
  u3_evlo* log_u;    //!< event log
  u3_meta  met_u;    //!< metadata
  struct {
    uv_timer_t tim_u; //!< timer
    c3_d       eve_d; //!< last saved
  } sav_u;            //!< snapshot timer
  u3_moat*     inn_u; //!< input stream
  u3_mojo*     out_u; //!< output stream
  u3_cue_xeno* sil_u; //!< cue handle
  enum {
    u3_mars_work_e = 0, //!< working
    u3_mars_save_e = 1, //!< snapshotting
    u3_mars_exit_e = 2, //!< exiting
  } sat_e;              //!< state
  c3_list* gif_u; //!< response queue of u3_gift (front=oldest, back=youngest)
  void (*xit_f)(void); //!< exit callback
} u3_mars;

//==============================================================================
// Functions
//==============================================================================

//! Boot a new ship.
c3_o
u3_mars_boot(c3_c* dir_c, u3_noun com);

//! Restart an existing ship.
u3_mars*
u3_mars_init(c3_c* dir_c, u3_moat* inn_u, u3_mojo* out_u);

//! Try to send a task into mars.
c3_o
u3_mars_kick(u3_mars* mar_u, c3_d len_d, c3_y* hun_y);

//! Garbage collect.
void
u3_mars_grab(void);

#endif /* ifndef U3_VERE_MARS_H */
