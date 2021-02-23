// The classes in this header define the common interface between your implementation and 
// the testing environment. Exactly the same implementation is present in the progtest's 
// testing environment. You are not supposed to modify any declaration in this file, 
// any change is likely to break the compilation.
#ifndef __common_h__22034590234652093456179823592__
#define __common_h__22034590234652093456179823592__

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>


static const constexpr uint32_t SHIFT_MSG_ID               = 37;
static const constexpr uint32_t SHIFT_PAYLOAD_LEN          = 32;
static const constexpr uint64_t MASK_PAYLOAD_LEN           = 0x1f;
static const constexpr uint64_t MASK_PAYLOAD               = 0xffffffff;
//=================================================================================================
/**
 * CBigInt is a simple implementation of a big-number class. The class handles positive integers of 
 * size 512 bits maximum. Supported operations are initialization, addition, multiplication, comparison, 
 * and a conversion-to-string.
 *
 * @note This interface is present in the progtest testing environment. 
 * @note CBigInt class and the overloaded operators are available in all tests, even in the bonus tests.
 *
 */
class CBigInt
{
  public:
    //---------------------------------------------------------------------------------------------
    /**
     * Initialize an object with a 64 bit unsigned integer (zero extended in the upper bits).
     * @param[in] val        value to set
     */
                             CBigInt                       ( uint64_t          val = 0);
    //---------------------------------------------------------------------------------------------
    /**
     * Initialize an object with a decimal integer.
     * @param[in] val        value to set
     * @exception std::invalid_argument if the input string is not a valid decimal integer.
     * @note overflow is silently ignored
     */
                             CBigInt                       ( std::string_view  val );
    //---------------------------------------------------------------------------------------------
    /**
     * Replace the contents with a 64 bit unsigned integer (zero extended in the upper bits).
     * @param[in] val        value to set
     * @return a reference to *this
     */
    CBigInt                & operator =                    ( uint64_t          val );
    //---------------------------------------------------------------------------------------------
    /**
     * Convert the value into a string (decimal notation)
     * @return a string representation of *this
     */
    std::string              ToString                      ( void ) const;
    //---------------------------------------------------------------------------------------------
    /**
     * Test whether the value is zero, or not
     * @return zero -> true, nonzero -> false
     */
    bool                     IsZero                        ( void ) const;
    //---------------------------------------------------------------------------------------------
    /**
     * Compare two big integers
     * @param[in] x          the value to compare to
     * @return one of: -1 (*this < x), 0 (*this == x), +1 (*this > x)
     */
    int32_t                  CompareTo                     ( const CBigInt   & x ) const;
    //---------------------------------------------------------------------------------------------
    /**
     * Add a CBigInt to *this
     * @param[in] x          the value to add
     * @return a reference to *this
     * @note overflow is ignored
     */
    CBigInt                & operator +=                   ( const CBigInt   & x );
    //---------------------------------------------------------------------------------------------
    /**
     * Multiply *this wint another CBigInt
     * @param[in] x          the multiplier
     * @return a reference to *this
     * @note overflow is ignored
     */
    CBigInt                & operator *=                   ( const CBigInt   & x );
  private:
    static const constexpr uint32_t BIGINT_BITS            = 512;
    static const constexpr uint32_t BIGINT_INTS            = BIGINT_BITS >> 5;
    uint32_t                 m_Data[ BIGINT_INTS ];
    void                     mulAdd                        ( uint32_t          st,
                                                             const uint32_t  * v, 
                                                             uint64_t          mul );
    uint32_t                 divMod                        ( uint32_t          x );
};
//-------------------------------------------------------------------------------------------------
/**
 * A convenience wrapper - sum two big integers
 */
inline CBigInt     operator +                              ( CBigInt           a,
                                                             const CBigInt   & b )
{
  return std::move ( a += b );
}
//-------------------------------------------------------------------------------------------------
/**
 * A convenience wrapper - multiply two big integers
 */
inline CBigInt     operator *                              ( CBigInt           a,
                                                             const CBigInt   & b )
{
  return std::move ( a *= b );
}
//=================================================================================================
/**
 * Calculate CRC32 checksum for the input bitfield. The calculation is performed with 1 bit granularity.
 * The bits are processed according to the CRC32 standard recommendation, i.e., starting from the least 
 * significant bits. CRC32 polynomial is 0x04c11db7
 *
 * @param[in] data  a pointer to the first byte in the bitfield
 * @param[in] bits  the number of bits in the input bitfield
 * @return 32 bit CRC value
 * @note This function is available in all tests, even in the bonus tests.
 */
uint32_t           CalculateCRC32                          ( const uint8_t   * data,
                                                             size_t            bits );
//=================================================================================================
class CReceiver : public std::enable_shared_from_this<CReceiver>
{
  public:
    virtual                  ~CReceiver                    ( void ) = default;
    virtual bool             Recv                          ( uint64_t        & data ) = 0;
};
typedef std::shared_ptr<CReceiver>                         AReceiver;
//=================================================================================================
class CTransmitter : public std::enable_shared_from_this<CTransmitter>
{
  public:
    virtual                  ~CTransmitter                 ( void ) = default;
    virtual void             Send                          ( uint32_t         id,
                                                             const CBigInt  & cnt) = 0;
    virtual void             Incomplete                    ( uint32_t          id ) = 0;
};
typedef std::shared_ptr<CTransmitter>                      ATransmitter;
//=================================================================================================

#endif /* __common_h__22034590234652093456179823592__ */
