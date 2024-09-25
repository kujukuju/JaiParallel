
#module_parameters(WASM := false);

parallel_init :: () {
    if threads_initialized {
        return;
    }
    threads_initialized = true;

    #if WASM {
        thread_count = 16;
    } else {
        thread_count = get_number_of_processors();
    }
    threads = NewArray(thread_count, Thread);

    for i: 0..threads.count - 1 {
        threads[i].data = New(ThreadData);

        thread_data := cast(*ThreadData) threads[i].data;
        // thread_data.index = i;
        init(*thread_data.run_semaphore);

        thread_init(*threads[i], thread_callback);
        thread_start(*threads[i]);
    }

    init(*pause_semaphore);
}

parallel_for :: (start: int, end_exclusive: int, $callback: (i: int, thread: int), thread_count: int = 0) {
    #if SINGLE_THREAD {
        for i: start..end_exclusive - 1 {
            callback(i, 0, data);
        }
        return;
    }

    if !threads_initialized {
        parallel_init();
    }

    if !thread_count {
        thread_count = threads.count;
    }

    // stack stuff is fine because this function waits for the threads to finish before proceeding
    baked_callback :: (thread: *Thread) {
        thread_data: *ThreadData = cast(*ThreadData) thread.data;

        start_index := thread_data.start_index;
        end_index_exclusive := thread_data.end_index_exclusive;

        thread_index := thread_data.thread_index;

        for index: start_index..end_index_exclusive - 1 {
            inline callback(index, thread_index);
        }
    }
    thread_baked_callback = baked_callback;

    finished_count = 0;

    increment_value := cast(int) ceil(cast(float) (end_exclusive - start) / thread_count);

    start_index := start;
    end_index_exclusive := start_index + increment_value;
    for thread_index: 0..thread_count - 1 {
        thread := threads[thread_index];
        thread_data := cast(*ThreadData) thread.data;
        thread_data.thread_index = thread_index;
        thread_data.start_index = start_index;
        thread_data.end_index_exclusive = end_index_exclusive;
        thread_data.requested_thread_count = thread_count;

        signal(*thread_data.run_semaphore);

        start_index = end_index_exclusive;
        end_index_exclusive = min(start_index + increment_value, end_exclusive);
    }

    // pause until threads say ready
    wait_for(*pause_semaphore);
}

parallel_for :: (start: int, end_exclusive: int, $callback: (i: int, thread: int, data: $T), data: T, thread_count: int = 0) {
    #if SINGLE_THREAD {
        for i: start..end_exclusive {
            callback(i, 0, data);
        }
        return;
    }

    if !threads_initialized {
        parallel_init();
    }

    if !thread_count {
        thread_count = threads.count;
    }

    // stack stuff is fine because this function waits for the threads to finish before proceeding
    baked_callback :: (thread: *Thread) {
        thread_data: *ThreadData = cast(*ThreadData) thread.data;

        start_index := thread_data.start_index;
        end_index_exclusive := thread_data.end_index_exclusive;

        thread_index := thread_data.thread_index;

        data := (cast(*T) thread_data.data).*;

        for index: start_index..end_index_exclusive - 1 {
            inline callback(index, thread_index, data);
        }
    }
    thread_baked_callback = baked_callback;

    finished_count = 0;

    increment_value := cast(int) ceil(cast(float) (end_exclusive - start) / thread_count);

    start_index := start;
    end_index_exclusive := start_index + increment_value;
    for thread_index: 0..thread_count - 1 {
        thread := threads[thread_index];
        thread_data := cast(*ThreadData) thread.data;
        thread_data.thread_index = thread_index;
        thread_data.start_index = start_index;
        thread_data.end_index_exclusive = end_index_exclusive;
        thread_data.requested_thread_count = thread_count;
        thread_data.data = *data;

        signal(*thread_data.run_semaphore);

        start_index = end_index_exclusive;
        end_index_exclusive = min(start_index + increment_value, end_exclusive);
    }

    // pause until threads say ready
    wait_for(*pause_semaphore);
}

get_thread_count :: () -> int {
    #if SINGLE_THREAD {
        return 1;
    }
    
    if !threads_initialized {
        parallel_init();
    }

    return thread_count;
}

// parallel_for :: (start: int, end_exclusive: int, callback: (i: int)) {
//     #if SINGLE_THREAD {
//         for i: start..end_exclusive - 1 {
//             callback(i);
//         }
//         return;
//     }

//     if !threads_initialized {
//         parallel_init();
//     }

//     // stack stuff is fine because this function waits for the threads to finish before proceeding
//     thread_baked_callback = callback;

//     finished_count = 0;

//     increment_value := cast(int) ceil(cast(float) (end_exclusive - start) / threads.count);

//     start_index := start;
//     end_index_exclusive := start_index + increment_value;
//     for thread_index: 0..threads.count - 1 {
//         thread := threads[thread_index];
//         thread_data := cast(*ThreadData) thread.data;
//         thread_data.start_index = start_index;
//         thread_data.end_index_exclusive = end_index_exclusive;

//         signal(*thread_data.run_semaphore);

//         start_index = end_index_exclusive;
//         end_index_exclusive = min(start_index + increment_value, end_exclusive);
//     }

//     // pause until threads say ready
//     wait_for(*pause_semaphore);
// }

#scope_file

SINGLE_THREAD :: false;

ThreadData :: struct {
    run_semaphore: Semaphore;
    thread_index: int;
    start_index: int;
    end_index_exclusive: int;
    requested_thread_count: int;
    data: *void;
}

thread_callback :: (thread: *Thread) -> s64 {
    thread_data: *ThreadData = cast(*ThreadData) thread.data;

    while true {
        // wait for the thread to be ready
        wait_for(*thread_data.run_semaphore);

        if thread_data.thread_index < thread_data.requested_thread_count {
            thread_baked_callback(thread);

            // data := thread_data.data;

            // for index: thread_data.start_index..thread_data.end_index_exclusive - 1 {
            //     thread_baked_callback(index, data);
            // }

            reset_temporary_storage();
            
            old_finished_count := atomic_add(*finished_count, 1);
            if old_finished_count == thread_data.requested_thread_count - 1 {
                signal(*pause_semaphore);
            }
        }
    }

    return 0;
}

thread_count: int;

pause_semaphore: Semaphore;
finished_count: int;

threads: [] Thread;
threads_initialized: bool;

// thread_baked_callback: #type (i: int, data: *void);
thread_baked_callback: #type (thread: *Thread);

#import "Basic";
#import "Thread";
#import "Atomics";
#import "System";
#import "Math";