use std::iter::Iterator;
use std::thread;

pub type Sender<T> = crossbeam_channel::Sender<T>;
pub type Receiver<T> = crossbeam_channel::Receiver<T>;

/// Marshall the output of parallel producers in sequence to a serial consumer
///
/// We are given a sequence of `producers` and a `consumer`.
///
/// The producers may be run in parallel in any order.
///
/// However, the consumer must be called sequentially with each of the
/// producers' output in the sequnce given to the argument to this function.
///
/// Each producer is a closure that takes the `Sender` part of a channel
/// and returns a 'task' closure which is to be run by this function in a separate
/// OS-level thread. The 'task' sends its output as a single message
/// to the channel and ends.
///
/// A separate OS-level thread receives from these channels and feeds the output in
/// sequence to the consumer closure.
///
/// At any time the total number of running producers plus the number
///   of outputs waiting to be consumed will be less than or equal to
///   `max_inflight`
///
pub fn seq_rw_marshall<D, T, I, C>(producers: &mut I, consumer: &mut C, max_inflight: usize)
where
    I: Iterator,
    I::Item: FnOnce(Sender<D>) -> T,    //Producer
    T: FnOnce() + Send, //Task
    C: FnMut(D) + Send,
    D: Send,
{
    //We must have at least one jon in flight to make progress
    assert!(max_inflight > 0);

    let mut tasks_remain = true;
    let mut inflight = 0usize;

    //Spawn a thread that calls the consumer.
    //
    //Channels from which data for the consumer is to be received
    //  are themselves received from the jobs channel.
    //
    //Ordering is enforced by ensuring that producer tasks
    //  are spawned in order, their channels are inserted into
    //  the jobs channel in order, and that the jobs channel
    //  is a FIFO (guaranteed by the channel library).
    //
    //After each job has been consumed a () is sent as an acknowledgement
    //  on the ack channel so that the main loop learns that it can
    //  spawn another producer.
    //
    thread::scope(|ts| {
        let (cons_jobs_s, cons_jobs_r) = crossbeam_channel::unbounded::<Receiver<D>>();
        let (cons_ack_s, cons_ack_r) = crossbeam_channel::unbounded::<()>();
    
        ts.spawn(move || {
            for ch in cons_jobs_r {
                let job = ch.recv().unwrap();
                consumer(job);
                cons_ack_s.send(()).unwrap();
            }
        });

        //Main loop
        //Spawn producers, wait for acks from consumer
        while tasks_remain || inflight > 0 {
            while tasks_remain && inflight <= max_inflight {
                match producers.next() {
                    None => {
                        tasks_remain = false;
                    }
                    Some(prod) => {
                        let (s, r) = crossbeam_channel::unbounded::<D>();
                        ts.spawn(prod(s));
                        cons_jobs_s.send(r).unwrap();
                        inflight += 1;
                    }
                }
            }
            if inflight > 0 {
                cons_ack_r.recv().unwrap();
                inflight -= 1;
            }
        }
    });
}
