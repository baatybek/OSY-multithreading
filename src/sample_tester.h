// The classes in this header are used in the example test. You are free to 
// modify these classes, add more test cases, and add more test sets.
// These classes do not exist in the progtest's testing environment.
#ifndef __sample_tester_h__0982345234598123452345__
#define __sample_tester_h__0982345234598123452345__

#include <functional>
#include <deque>
#include "common.h"

class CExampleReceiver : public CReceiver
{
  public:
                             CExampleReceiver              ( std::initializer_list<uint64_t> data );
    virtual bool             Recv                          ( uint64_t        & fragment ) override;
  private:
    std::deque<uint64_t>     m_Data;
};

class CExampleTransmitter : public CTransmitter
{
  public:
                             CExampleTransmitter           ( void ) = default;
    virtual void             Send                          ( uint32_t          id,
                                                             const CBigInt   & cnt ) override;
    virtual void             Incomplete                    ( uint32_t          id ) override;
  
  
    size_t                   TotalSent                     ( void ) const;
    size_t                   TotalIncomplete               ( void ) const;
  private:
    size_t                   m_Sent                        = 0;
    size_t                   m_Incomplete                  = 0;
};

struct CSampleData
{
  uint32_t                    m_ID;
  const char                * m_Result;
  std::initializer_list<uint64_t>  m_Fragments;
};

void               FragmentSender                          ( std::function<void(uint64_t)> target,
                                                             std::initializer_list<uint64_t> data );

extern std::initializer_list<CSampleData>                  g_TestSets;

#endif /* __sample_tester_h__0982345234598123452345__ */
