#ifndef __PROGTEST__
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <climits>
#include <cfloat>
#include <cassert>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <vector>
#include <set>
#include <list>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <stack>
#include <deque>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include "progtest_solver.h"
#include "sample_tester.h"
using namespace std;
#endif /* __PROGTEST__ */

struct CMessage {
    bool complete;
    CBigInt  result;
    uint32_t message_id;
    vector < uint64_t > m_fragments;
    CMessage () {}
    CMessage ( uint32_t message_id, uint64_t fragment ) {
        complete = false;
        this->message_id = message_id;
        m_fragments.emplace_back( fragment );
    }
};

class CSentinelHacker
{
    mutex m_mutex;
    unsigned receivers_count;
    unsigned transmitters_count;
    unsigned workers_count;
    unsigned real_workers_count;
    vector <thread> receiver_threads;
    vector <thread> worker_threads;
    vector <thread> transmitter_threads;
    vector <AReceiver> receivers;
    vector <ATransmitter> transmitters;
    queue<uint64_t> fragmentsQ;
    condition_variable cv_receiver_worker;
    condition_variable cv_worker;
    condition_variable cv_worker_transmitter;
    map < uint32_t, CMessage > map_msg_to_process;
    queue < pair < uint32_t, CBigInt > > worker_completed_msg_id;
    queue < uint32_t > worker_incomplete_messages;
    map < uint32_t, CMessage >::iterator chechIncompleteMssg;
    void IncompleteMessagesWorker ();
    map < uint32_t, CMessage >::iterator InsertFragmentToProcess(const uint64_t &fragment);
    void Receiver ( AReceiver receiver );
    void Worker ();
    void Transmitter ( ATransmitter transmitter);
public:
    static bool SeqSolve  ( const vector<uint64_t> & fragments, CBigInt & res );
    void AddTransmitter ( ATransmitter x );
    void AddReceiver ( AReceiver x );
    void AddFragment ( uint64_t x );
    void Start ( unsigned thrCount );
    void Stop ( void );
};
// TODO: CSentinelHacker implementation goes here

/**
 * Receiver tries continuously to get a fragment from AReceiver receiver
 * by passing a local variable to the CReceiver::Recv ( uint64_t & )
 * if CReceiver::Recv returns false, then there is no fragments to receive, so it finishes it's job
 * and decrements number of active receivers count
 * if CReceiver::Recv returns true, then it passes newly received fragment
 * to CSentinelHacker::AddFragment ( uint64_t ), where CSentinelHacker::AddFragment stores the fragment in fragmentsQ
 *
 * @param[in] AReceiver receiver is shared_ptr<CReceiver> instance
 */
void CSentinelHacker::Receiver ( AReceiver receiver ) {
    while( true ) {
        uint64_t fragment;
        if( ! receiver->Recv( fragment ) ) {
            break;
        }
        CSentinelHacker::AddFragment( fragment );
    }
}

void CSentinelHacker::Worker() {
    while ( true ) {
        unique_lock <mutex> u_lock ( m_mutex );
        cv_receiver_worker.wait( u_lock, [this] () { return !fragmentsQ.empty() || receivers_count == 0; });
        if( fragmentsQ.empty() ) {
           // printf("WORKERS_WHILE_LOOP IS BREAKING!!!!!\n");
            break;
        } else {
            uint64_t c_fragment = fragmentsQ.front();
            fragmentsQ.pop();
            u_lock.unlock();
            map < uint32_t, CMessage >::iterator it_msg_map = InsertFragmentToProcess(c_fragment);
            u_lock.lock();
            vector<uint64_t> fragments_local = it_msg_map->second.m_fragments;
            u_lock.unlock();
            CBigInt res;
            if ( SeqSolve( fragments_local, res ) ) {
                u_lock.lock();
                it_msg_map->second.complete = true;
                it_msg_map->second.result = res;
                worker_completed_msg_id.push( make_pair ( it_msg_map->first, res ) );
                cv_worker_transmitter.notify_one(); //
            }
        }
    }
    {
        unique_lock<mutex> u_lock(m_mutex);
        //printf("SCOPE\n");
        workers_count--;
        if (workers_count == 0) {
            chechIncompleteMssg = map_msg_to_process.begin();
            cv_worker.notify_all();

        } else {
            cv_worker.wait(u_lock, [this]() { return workers_count == 0; });
        }
    }



    while ( true ) {
        unique_lock<mutex> u_lock(m_mutex);
       // printf("before if\n");
        if ( chechIncompleteMssg == map_msg_to_process.end() ) {
            break;
        }
        if ( ! chechIncompleteMssg->second.complete ) {
            uint32_t id = chechIncompleteMssg->first;
            worker_incomplete_messages.push( id );
            cv_worker_transmitter.notify_one();
        }
        chechIncompleteMssg++;
    }

   // printf("Worker is done\n" );
}

void CSentinelHacker::Transmitter ( ATransmitter transmitter) {
    while ( true ) {
        unique_lock <mutex> u_lock ( m_mutex );
      //  printf("before wait\n");
      //  printf("Completed messages: %d\n", worker_completed_msg_id.empty() );
      //  printf("Incomplete messages: %d\n", worker_incomplete_messages.empty() );
      //  printf("Worker's count: %u\n", real_workers_count );

        cv_worker_transmitter.wait( u_lock, [this] () { return !worker_completed_msg_id.empty() || ! worker_incomplete_messages.empty() || real_workers_count == 0; } );
      //  printf("after wait\n");
        if( worker_incomplete_messages.empty() && worker_completed_msg_id.empty() )
            break;
        if( ! worker_completed_msg_id.empty() ) {
            pair < uint32_t , CBigInt > message = worker_completed_msg_id.front();
            worker_completed_msg_id.pop();
            u_lock.unlock();
            transmitter->Send( message.first, message.second );
            u_lock.lock();
        }



        if( ! worker_incomplete_messages.empty() ) {
            uint32_t message_id = worker_incomplete_messages.front();
            worker_incomplete_messages.pop();
            u_lock.unlock();
            transmitter->Incomplete( message_id );
        }
    }
    //  wait for transmitter queue or workers == 0
    // if ( messages_complete ) -> transmitter->Send...
    // else transmitter->incomplete ///
    // if ( queue is empty and real worker count == 0 ) break;
}

map < uint32_t, CMessage >::iterator CSentinelHacker::InsertFragmentToProcess(const uint64_t &fragment) {
    unique_lock <mutex> u_lock ( m_mutex );
    uint32_t message_id = fragment >> SHIFT_MSG_ID;
    map < uint32_t, CMessage >::iterator msg_iterator = map_msg_to_process.find( message_id );
    if( msg_iterator == map_msg_to_process.end() ) {
        map_msg_to_process.emplace( make_pair( message_id, CMessage ( message_id, fragment ) ) );
        msg_iterator = map_msg_to_process.find( message_id );
    } else {
        msg_iterator->second.m_fragments.emplace_back( fragment );
    }
    return msg_iterator;
}

//-------------------------------------------------------------------------------------------------

bool CSentinelHacker::SeqSolve ( const vector<uint64_t> & fragments, CBigInt & res ) {
    /**
     * @payload: (const uint8_t *) the entire payload of the message: i.e. CRC32 followed by the bitfield
     * @length: (size_t) the length of the payload in bits
     * Lambda Expression counts the number of expression can be constructed by CountExpressions and
     * if the new count of expression exceed the one given in CBig & res, we assign res to the new result
     */
    auto recvFn = [& res] ( const uint8_t * payload, size_t length ) {
        CBigInt expressionCount = CountExpressions( payload + 4, length - 32 ); // CountExpressions returns the number of boolean expressions found
        if( res.CompareTo( expressionCount ) == -1 )
            res = expressionCount;
    };
    /**
     * FindPermutations returns the number of different valid payloads extracted (i.e., the total # times the callback was actually called).
     * 0 is returned if the input fragments do not form a valid  message (e.g. some fragments are missing)
     * or if the input fragments mix two or more messages with different IDs.
     */
    return FindPermutations( fragments.data(), fragments.size(), recvFn );
}

void CSentinelHacker::AddTransmitter ( ATransmitter x ) {
    transmitters.push_back(x);
}

void CSentinelHacker::AddReceiver ( AReceiver x ) {
    receivers.push_back(x);
}

void CSentinelHacker::AddFragment ( uint64_t x ) {
    unique_lock<mutex> u_lock ( m_mutex );
    fragmentsQ.push( x );
    cv_receiver_worker.notify_one();
}

void CSentinelHacker::Start( unsigned thrCount ) {
    workers_count = thrCount;
    real_workers_count = thrCount;
    receivers_count = receivers.size();
    transmitters_count = transmitters.size();
    for( unsigned i = 0; i < receivers_count; i++ ) {
        receiver_threads.emplace_back( thread( &CSentinelHacker::Receiver, this, receivers[i] ) );
    }
    for( unsigned i = 0; i < thrCount; i++ ) {
        worker_threads.emplace_back( thread( &CSentinelHacker::Worker, this ) );
    }
    for( unsigned i = 0; i < transmitters_count; i++ ) {
        transmitter_threads.emplace_back( &CSentinelHacker::Transmitter, this, transmitters[i] );
    }
}
// join
void CSentinelHacker::Stop ( void ){
    for( unsigned i = 0; i < receiver_threads.size(); i++ ) {
        receiver_threads[i].join();
    }

    unique_lock <mutex> u_lock ( m_mutex );
    receivers_count = 0;
    cv_receiver_worker.notify_all();
    u_lock.unlock();

    for( unsigned i = 0; i < worker_threads.size(); i++ ) {
        worker_threads[i].join();
    }

    u_lock.lock();

    real_workers_count = 0;
    cv_worker_transmitter.notify_all();
    u_lock.unlock();

    for(unsigned i = 0; i < transmitter_threads.size(); i++ ) {
        transmitter_threads[i].join();
    }
}
//-------------------------------------------------------------------------------------------------

#ifndef __PROGTEST__
int                main                                    ( void )
{
    using namespace std::placeholders;
    for ( const auto & x : g_TestSets )
    {
        CBigInt res;
        assert ( CSentinelHacker::SeqSolve ( x . m_Fragments, res ) );
        assert ( CBigInt ( x . m_Result ) . CompareTo ( res ) == 0 );
    }

    CSentinelHacker test;
    auto            trans = make_shared<CExampleTransmitter> ();
    AReceiver       recv  = make_shared<CExampleReceiver> ( initializer_list<uint64_t> { 0x02230000000c, 0x071e124dabef, 0x02360037680e, 0x071d2f8fe0a1, 0x055500150755 } );

    test . AddTransmitter ( trans );
    test . AddReceiver ( recv );
    test . Start ( 3 );

    static initializer_list<uint64_t> t1Data = { 0x071f6b8342ab, 0x0738011f538d, 0x0732000129c3, 0x055e6ecfa0f9, 0x02ffaa027451, 0x02280000010b, 0x02fb0b88bc3e };
    thread t1 ( FragmentSender, bind ( &CSentinelHacker::AddFragment, &test, _1 ), t1Data );

    static initializer_list<uint64_t> t2Data = { 0x073700609bbd, 0x055901d61e7b, 0x022a0000032b, 0x016f0000edfb };
    thread t2 ( FragmentSender, bind ( &CSentinelHacker::AddFragment, &test, _1 ), t2Data );
    FragmentSender ( bind ( &CSentinelHacker::AddFragment, &test, _1 ), initializer_list<uint64_t> { 0x017f4cb42a68, 0x02260000000d, 0x072500000025 } );
    t1 . join ();
    t2 . join ();
    test . Stop ();
    assert ( trans -> TotalSent () == 4 );
    assert ( trans -> TotalIncomplete () == 2 );

    return 0;
}
#endif /* __PROGTEST__ */
