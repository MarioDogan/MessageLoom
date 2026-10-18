// ThreadSanitizer suppressions for known reports inside libzmq, which is not TSan-annotated.
// Linked into tests only.

extern "C" const char* __tsan_default_suppressions() {
  // zmq_ctx_term() resolves inproc connections whose endpoint was never bound while the reaper
  // thread tears the same pipes down; the race is internal to libzmq's shutdown.
  return "race:zmq::ctx_t::connect_inproc_sockets\n";
}
