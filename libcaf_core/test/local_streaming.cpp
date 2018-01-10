/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2017                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#define CAF_SUITE local_streaming

#include <memory>
#include <numeric>

#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/broadcast_scatterer.hpp"
#include "caf/buffered_scatterer.hpp"
#include "caf/downstream_msg.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/inbound_path.hpp"
#include "caf/mailbox_element.hpp"
#include "caf/no_stages.hpp"
#include "caf/outbound_path.hpp"
#include "caf/scheduled_actor.hpp"
#include "caf/send.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/stream_manager.hpp"
#include "caf/stream_scatterer.hpp"
#include "caf/stream_slot.hpp"
#include "caf/system_messages.hpp"
#include "caf/upstream_msg.hpp"
#include "caf/variant.hpp"

#include "caf/policy/arg.hpp"

#include "caf/mixin/sender.hpp"

#include "caf/test/dsl.hpp"

#include "caf/intrusive/drr_queue.hpp"
#include "caf/intrusive/singly_linked.hpp"
#include "caf/intrusive/task_result.hpp"
#include "caf/intrusive/wdrr_dynamic_multiplexed_queue.hpp"
#include "caf/intrusive/wdrr_fixed_multiplexed_queue.hpp"

#include "caf/detail/gcd.hpp"
#include "caf/detail/overload.hpp"
#include "caf/detail/stream_sink_impl.hpp"
#include "caf/detail/stream_source_impl.hpp"
#include "caf/detail/stream_stage_impl.hpp"
#include "caf/detail/tick_emitter.hpp"

using std::string;
using std::vector;

using namespace caf;
using namespace caf::intrusive;
namespace {

struct file_reader_state {
  static const char* name;
};

const char* file_reader_state::name = "file_reader";

behavior file_reader(stateful_actor<file_reader_state>* self) {
  using buf = std::deque<int>;
  return {
    [=](string& fname) -> output_stream<int, string> {
      CAF_CHECK_EQUAL(fname, "numbers.txt");
      CAF_CHECK_EQUAL(self->mailbox().empty(), true);
      return self->make_source(
        // forward file name in handshake to next stage
        std::forward_as_tuple(std::move(fname)),
        // initialize state
        [&](buf& xs) {
          xs = buf{1, 2, 3, 4, 5, 6, 7, 8, 9};
        },
        // get next element
        [=](buf& xs, downstream<int>& out, size_t num) {
          CAF_MESSAGE("push " << num << " more messages downstream");
          auto n = std::min(num, xs.size());
          for (size_t i = 0; i < n; ++i)
            out.push(xs[i]);
          xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
        },
        // check whether we reached the end
        [=](const buf& xs) {
          return xs.empty();
        }
      );
    }
  };
}

struct sum_up_state {
  static const char* name;
};

const char* sum_up_state::name = "sum_up";

behavior sum_up(stateful_actor<sum_up_state>* self) {
  return {
    [=](stream<int>& in, string& fname) {
      CAF_CHECK_EQUAL(fname, "numbers.txt");
      return self->make_sink(
        // input stream
        in,
        // initialize state
        [](int& x) {
          x = 0;
        },
        // processing step
        [](int& x, int y) {
          x += y;
        },
        // cleanup and produce result message
        [](int& x) -> int {
          return x;
        }
      );
    }
  };
}

/*

struct streamer_state {
  static const char* name;
};

const char* streamer_state::name = "streamer";

void streamer(stateful_actor<streamer_state>* self, const actor& dest) {
  using buf = std::deque<int>;
  self->make_source(
    // destination of the stream
    dest,
    // "file name" as seen by the next stage
    std::make_tuple("numbers.txt"),
    // initialize state
    [&](buf& xs) {
      xs = buf{1, 2, 3, 4, 5, 6, 7, 8, 9};
    },
    // get next element
    [=](buf& xs, downstream<int>& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // check whether we reached the end
    [=](const buf& xs) {
      return xs.empty();
    },
    // handle result of the stream
    [=](expected<int>) {
      // nop
    }
  );
}

struct filter_state {
  static const char* name;
};

const char* filter_state::name = "filter";

behavior filter(stateful_actor<filter_state>* self) {
  return {
    [=](stream<int>& in, string& fname) -> stream<int> {
      CAF_CHECK_EQUAL(fname, "numbers.txt");
      return self->make_stage(
        // input stream
        in,
        // forward file name in handshake to next stage
        std::forward_as_tuple(std::move(fname)),
        // initialize state
        [=](unit_t&) {
          // nop
        },
        // processing step
        [=](unit_t&, downstream<int>& out, int x) {
          if ((x & 0x01) != 0)
            out.push(x);
        },
        // cleanup
        [=](unit_t&) {
          // nop
        }
      );
    }
  };
}

struct broken_filter_state {
  static const char* name;
};

const char* broken_filter_state::name = "broken_filter";

behavior broken_filter(stateful_actor<broken_filter_state>*) {
  return {
    [=](stream<int>& x, const string& fname) -> stream<int> {
      CAF_CHECK_EQUAL(fname, "numbers.txt");
      return x;
    }
  };
}

struct drop_all_state {
  static const char* name;
};

const char* drop_all_state::name = "drop_all";

behavior drop_all(stateful_actor<drop_all_state>* self) {
  return {
    [=](stream<int>& in, string& fname) {
      CAF_CHECK_EQUAL(fname, "numbers.txt");
      return self->make_sink(
        // input stream
        in,
        // initialize state
        [](unit_t&) {
          // nop
        },
        // processing step
        [](unit_t&, int) {
          // nop
        },
        // cleanup and produce void "result"
        [](unit_t&) {
          CAF_LOG_INFO("drop_all done");
        }
      );
    }
  };
}

struct nores_streamer_state {
  static const char* name;
};

const char* nores_streamer_state::name = "nores_streamer";

void nores_streamer(stateful_actor<nores_streamer_state>* self,
                             const actor& dest) {
  CAF_LOG_INFO("nores_streamer initialized");
  using buf = std::deque<int>;
  self->make_source(
    // destination of the stream
    dest,
    // "file name" for the next stage
    std::make_tuple("numbers.txt"),
    // initialize state
    [&](buf& xs) {
      xs = buf{1, 2, 3, 4, 5, 6, 7, 8, 9};
    },
    // get next element
    [=](buf& xs, downstream<int>& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // check whether we reached the end
    [=](const buf& xs) {
      return xs.empty();
    },
    // handle result of the stream
    [=](expected<void>) {
      // nop
    }
  );
}
*/

struct fixture : test_coordinator_fixture<> {

};

} // namespace <anonymous>

// -- unit tests ---------------------------------------------------------------

CAF_TEST_FIXTURE_SCOPE(local_streaming_tests, fixture)

CAF_TEST(depth_2_pipeline) {
  auto src = sys.spawn(file_reader);
  auto snk = sys.spawn(sum_up);
  auto pipeline = snk * src;
  CAF_MESSAGE(CAF_ARG(self) << CAF_ARG(src) << CAF_ARG(snk));
  // self --("numbers.txt")-----------> source                             sink
  self->send(pipeline, "numbers.txt");
  expect((string), from(self).to(src).with("numbers.txt"));
  // self                               source --(open_stream_msg)-------> sink
  expect((open_stream_msg), from(self).to(snk));
  // self                               source <--------------(ack_open)-- sink
  expect((upstream_msg::ack_open), from(snk).to(src));
}

CAF_TEST_FIXTURE_SCOPE_END()
