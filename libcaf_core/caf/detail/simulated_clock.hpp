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

#ifndef CAF_DETAIL_SIMULATED_CLOCK_HPP
#define CAF_DETAIL_SIMULATED_CLOCK_HPP

#include <chrono>

#include "caf/atom.hpp"

namespace caf {
namespace detail {

/// Converts realtime into a series of ticks, whereas each tick represents a
/// preconfigured timespan. For example, a tick emitter configured with a
/// timespan of 25ms generates a tick every 25ms after starting it.
class simulated_clock {
public:
  // -- member types -----------------------------------------------------------

  /// Discrete point in time.
  using clock_type = std::chrono::steady_clock;

  /// Discrete point in time.
  using time_point = typename clock_type::time_point;

  /// Discrete point in time.
  using duration_type = typename clock_type::duration;

  // -- constructors, destructors, and assignment operators --------------------

  simulated_clock() = default;

  virtual ~simulated_clock();

  // -- observers --------------------------------------------------------------

  virtual time_point now() const noexcept = 0;

  /// Returns the difference between `t0` and `t1`, allowing the clock to
  /// return any arbitrary value depending on the measurement that took place.
  virtual duration_type difference(atom_value measurement, time_point t0,
                                   time_point t1) const noexcept;
};

} // namespace detail
} // namespace caf

#endif // CAF_DETAIL_SIMULATED_CLOCK_HPP

