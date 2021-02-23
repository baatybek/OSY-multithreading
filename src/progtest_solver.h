#ifndef __progtest_solver_h__132487812734523145234__
#define __progtest_solver_h__132487812734523145234__

#include <cstdint>
#include <functional>
#include "common.h"

//-------------------------------------------------------------------------------------------------
/**
 * The function combines fragments of a single message and tries to recover the message payload:
 *  - the function checks that all fragments belong to the same message
 *  - for each permutation, the payload is extracted, serialized and checked for valid CRC 
 *  - if the CRC matches, then the message payload (CRC + bitfield) is passes to the callback
 *
 * @param [in] fragments   the fragments to process
 * @param [in] cnt         the # of fragments 
 * @param      recvFn      a callback to call for each permutation where the CRC matches. The callback is given
 *                         the payload (1-st argument: const uint8_t *) and the length of the payload in bits 
 *                         (2-nd argument: size_t). The callback is given the entire message payload, i.e., 
 *                         CRC32 followed by the bitfield. The length parameter is set to the length of the 
 *                         entire payload, i.e., 32 must be subtracted to get the # of bits in the bitfield.
 * @return the number of different valid payloads extracted (i.e., the total # times the callback was actually called). 
 *         0 is returned if the input fragments do not form a valid  message (e.g. some fragments are missing) 
 *         or if the input fragments mix two or more messages with different IDs. 
 * @note the function is rather slow - the time complexity is n! (check all permutations). 
 * @note the function is usable in mandatory and optional tests. The implementation in the Progtest environment 
 *       does not work correctly in the bonus test.
 */
uint32_t           FindPermutations                        ( const uint64_t  * fragments,
                                                             size_t            cnt,
                                                             std::function<void( const uint8_t *, size_t)> recvFn );
//-------------------------------------------------------------------------------------------------
/**
 * Given a bitfield representing boolean values, the function adds boolean operators &, | and ^ to 
 * form a valid Boolean expression in a prefix notation. The result is the number of prefix Boolean 
 * expressions that evaluate to true.
 *
 * @param [in] data   a pointer to the input bitfield (the processing starts with the least 
 *                    significant bit of the byte data points to)
 * @param [in] bits   the number of bits in the input bitfield
 * @return the number of boolean expressions found
 * @note the function is rather slow - the time complexity is ~n^3. 
 * @note the function is usable in mandatory and optional tests. The implementation in the Progtest environment 
 *       does not work correctly in the bonus test.
 */
CBigInt            CountExpressions                        ( const uint8_t   * data,
                                                             size_t            bits );
//-------------------------------------------------------------------------------------------------
 
#endif /* __progtest_solver_h__132487812734523145234__ */
