/*
Copyright (c) 2005-2011, NumPy Developers.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
       copyright notice, this list of conditions and the following
       disclaimer in the documentation and/or other materials provided
       with the distribution.

    * Neither the name of the NumPy Developers nor the names of any
       contributors may be used to endorse or promote products derived
       from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef __NPY_HALFFLOAT_H__
#define __NPY_HALFFLOAT_H__

/* jk: Minimal modifications to get this code to compile outside of numpy. */
#include <inttypes.h>

typedef uint16_t npy_half;
typedef uint16_t npy_uint16;
typedef int16_t npy_int16;
typedef uint32_t npy_uint32;
typedef uint64_t npy_uint64;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Half-precision routines
 */

/* Conversions */
float npy_half_to_float(npy_half h);
double npy_half_to_double(npy_half h);
npy_half npy_float_to_half(float f);
npy_half npy_double_to_half(double d);
/* Comparisons */
int npy_half_eq(npy_half h1, npy_half h2);
int npy_half_ne(npy_half h1, npy_half h2);
int npy_half_le(npy_half h1, npy_half h2);
int npy_half_lt(npy_half h1, npy_half h2);
int npy_half_ge(npy_half h1, npy_half h2);
int npy_half_gt(npy_half h1, npy_half h2);
/* faster *_nonan variants for when you know h1 and h2 are not NaN */
int npy_half_eq_nonan(npy_half h1, npy_half h2);
int npy_half_lt_nonan(npy_half h1, npy_half h2);
int npy_half_le_nonan(npy_half h1, npy_half h2);
/* Miscellaneous functions */
int npy_half_iszero(npy_half h);
int npy_half_isnan(npy_half h);
int npy_half_isinf(npy_half h);
int npy_half_isfinite(npy_half h);
int npy_half_signbit(npy_half h);
npy_half npy_half_copysign(npy_half x, npy_half y);
npy_half npy_half_spacing(npy_half h);
npy_half npy_half_nextafter(npy_half x, npy_half y);

/*
 * Half-precision constants
 */

#define NPY_HALF_ZERO   (0x0000u)
#define NPY_HALF_PZERO  (0x0000u)
#define NPY_HALF_NZERO  (0x8000u)
#define NPY_HALF_ONE    (0x3c00u)
#define NPY_HALF_NEGONE (0xbc00u)
#define NPY_HALF_PINF   (0x7c00u)
#define NPY_HALF_NINF   (0xfc00u)
#define NPY_HALF_NAN    (0x7e00u)

#define NPY_MAX_HALF    (0x7bffu)

/*
 * Bit-level conversions
 */

npy_uint16 npy_floatbits_to_halfbits(npy_uint32 f);
npy_uint16 npy_doublebits_to_halfbits(npy_uint64 d);
npy_uint32 npy_halfbits_to_floatbits(npy_uint16 h);
npy_uint64 npy_halfbits_to_doublebits(npy_uint16 h);

#ifdef __cplusplus
}
#endif

#endif
