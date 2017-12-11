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

#define CAF_SUITE simulated_clock

#include "caf/test/dsl.hpp"

#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/scoped_actor.hpp"

#include "caf/detail/simulated_clock.hpp"

using namespace caf;
using namespace std;

using time_atom = atom_constant<atom("time")>;
using realtime_atom = atom_constant<atom("realtime")>;

namespace {

detail::simulated_clock::time_point global_time;
std::chrono::seconds fake_diff{10};
std::chrono::seconds diff{100};

class mock_clock : public detail::simulated_clock {
public:
  time_point now() const noexcept override {
    return global_time;
  }
  duration_type difference(atom_value, time_point,
                           time_point) const noexcept override {
    return fake_diff;
  }
};

behavior testee(event_based_actor* self) {
  return {
    [=](time_atom) {
      CAF_CHECK_EQUAL(self->now(), global_time);
    },
    [=](realtime_atom) {
      // This check has a false negative if someone sets the system time to 0.
      self->unsetf(abstract_actor::has_simulated_time_flag);
      CAF_CHECK_NOT_EQUAL(self->now(), global_time);
    }
  };
}

using fixture = test_coordinator_fixture<actor_system_config, mock_clock>;

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(simulated_clock_tests, fixture)

CAF_TEST(system_access) {
  CAF_CHECK_EQUAL(sys.clock().now(), global_time);
  global_time += std::chrono::seconds(100);
  CAF_CHECK_EQUAL(sys.clock().now(), global_time);
}

CAF_TEST(scoped_actor_access) {
  auto t0 = global_time;
  CAF_CHECK_EQUAL(self->now(), global_time);
  global_time += std::chrono::seconds(100);
  CAF_CHECK_EQUAL(self->now(), global_time);
  CAF_CHECK_EQUAL(self->difference(atom(""), t0, global_time), fake_diff);
  // This check has a false negative if someone sets the system time to 0.
  self->unsetf(abstract_actor::has_simulated_time_flag);
  CAF_CHECK_NOT_EQUAL(self->now(), global_time);
  CAF_CHECK_EQUAL(self->difference(atom(""), t0, global_time), diff);
}

CAF_TEST(event_based_actor_access) {
  auto t = sys.spawn(testee);
  anon_send(t, time_atom::value);
  sched.run();
  global_time += std::chrono::seconds(100);
  anon_send(t, time_atom::value);
  sched.run();
  anon_send(t, realtime_atom::value);
  sched.run();
}

CAF_TEST_FIXTURE_SCOPE_END()
