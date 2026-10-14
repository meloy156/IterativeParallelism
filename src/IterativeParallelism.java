import info.kgeorgiy.java.advanced.iterative.ListIP;
import info.kgeorgiy.java.advanced.iterative.NewListIP;
import info.kgeorgiy.java.advanced.iterative.NewScalarIP;
import info.kgeorgiy.java.advanced.iterative.ListIP;

import java.lang.management.ThreadInfo;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IterativeParallelism implements NewListIP {


    @Override
    public <T> int argMax(int threads, List<T> values, Comparator<? super T> comparator) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(comparator, "comparator");  // argMax/argMin

        int sizeList = values.size();

        // Проверка исходных данных
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        // создание и запуск потоков
        int[] result = new int[threads];
        Thread[] pool = new Thread[threads];

        for (int k = 0; k < threads; k++) {
            final int numberThread = k;
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;

            pool[k] = new Thread(() -> {
                int indexMaxValue = indexStart;
                T maxValues = values.get(indexStart);
                for (int j = indexStart + 1; j < indexEnd; ++j) {
                    if (comparator.compare(values.get(j), maxValues) > 0) {
                        maxValues = values.get(j);
                        indexMaxValue = j;
                    }
                }
                result[numberThread] = indexMaxValue;
            });

            pool[k].start();
        }

        // ждем каждый поток
        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        // ищем глобальный максимум
        int indexGlobalMaxValue = result[0];
        T globalMaxValue = values.get(result[0]);
        for (int i : result) {
            if (comparator.compare(values.get(i), globalMaxValue) > 0) {
                indexGlobalMaxValue = i;
                globalMaxValue = values.get(i);
            }
        }


        return indexGlobalMaxValue;
    }


    @Override
    public <T> int argMin(int threads, List<T> values, Comparator<? super T> comparator) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(comparator, "comparator");  // argMax/argMin

        int sizeList = values.size();

        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        Thread[] pool = new Thread[threads];

        int[] result = new int[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[numberThread] = new Thread(() -> {
                int indexMinValue = indexStart;
                T minValue = values.get(indexStart);

                for (int j = indexStart + 1; j < indexEnd; j++) {
                    if (comparator.compare(values.get(j), minValue) < 0) {
                        indexMinValue = j;
                        minValue = values.get(j);
                    }
                }

                result[numberThread] = indexMinValue;
            });


            pool[k].start();
        }

        // ждем каждый поток
        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        // считаем самый мин
        int indexGlobalMinValues = result[0];
        T globalMinValue = values.get(result[0]);
        for (int i : result) {
            if (comparator.compare(values.get(i), globalMinValue) < 0) {
                indexGlobalMinValues = i;
                globalMinValue = values.get(i);
            }
        }

        return indexGlobalMinValues;
    }

    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // arices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // ind

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        int[] result = new int[threads];
        Arrays.fill(result, Integer.MAX_VALUE);
        Thread[] pool = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[k] = new Thread(() -> {
                for (int j = indexStart; j < indexEnd; j++) {
                    if (predicate.test(values.get(j))) {
                        result[numberThread] = j;
                        break;
                    }
                }
            });

            pool[k].start();
        }

        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        int indexMin = Integer.MAX_VALUE;
        for (int i : result) {
            if (i < indexMin)
                indexMin = i;
        }

        return indexMin < Integer.MAX_VALUE ? indexMin : -1;
    }


    @Override
    public <T> int lastIndexOf(int threads, List<T> values, Predicate<? super T> predicate) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;


        AtomicInteger resultIndex = new AtomicInteger(-1);
        Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;

            root[k] = new Thread(() -> {
                for (int j = indexStart; j < indexEnd; j++) {
                    if (predicate.test(values.get(j))) {
                        synchronized (resultIndex) {
                            if (j > resultIndex.get())
                                resultIndex.set(j);
                        }
                    }
                }
            });

            root[k].start();
        }

        try {
            for (Thread t : root) {
                if (t != null) {
                    t.join();
                }
            }
        } catch (InterruptedException e) {
            for (Thread t : root) {
                if (t != null) {
                    t.interrupt();
                }
            }

            throw e;
        }

        return resultIndex.get();
    }


    @Override
    public <T> long sumIndices(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;


        AtomicLong resultIndex = new AtomicLong(0);
        Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;


            root[k] = new Thread(() -> {
                for (int j = indexStart; j < indexEnd; j++) {
                    if (predicate.test(values.get(j))) {
                        resultIndex.addAndGet(j);
                    }
                }
            });

            root[k].start();
        }


        try {
            for (Thread t : root) {
                if (t != null) {
                    t.join();
                }
            }
        } catch (InterruptedException e) {
            for (Thread t : root) {
                if (t != null) {
                    t.interrupt();
                }
            }

            throw e;
        }


        return resultIndex.get();
    }


    @Override
    public <T> int argMax(int threads, List<T> values, Comparator<? super T> comparator, int step) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(comparator, "comparator");  // arg

        int sizeList = values.size();

        // Проверка исходных данных
        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        // создание и запуск потоков
        int[] result = new int[threads];
        Thread[] pool = new Thread[threads];

        for (int k = 0; k < threads; k++) {
            final int numberThread = k;
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;

            pool[k] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;
                if (start >= indexEnd) return;

                int indexMaxValue = start;
                T maxValues = values.get(start);
                for (int j = start; j < indexEnd; j += step) {
                    if (comparator.compare(values.get(j), maxValues) > 0) {
                        maxValues = values.get(j);
                        indexMaxValue = j;
                    }
                }
                result[numberThread] = indexMaxValue;
            });

            pool[k].start();
        }

        // ждем каждый поток
        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        // ищем глобальный максимум
        int indexGlobalMaxValue = result[0];
        T globalMaxValue = values.get(result[0]);
        for (int i : result) {
            if (comparator.compare(values.get(i), globalMaxValue) > 0) {
                indexGlobalMaxValue = i;
                globalMaxValue = values.get(i);
            }
        }


        return indexGlobalMaxValue;
    }


    @Override
    public <T> int argMin(int threads, List<T> values, Comparator<? super T> comparator, int step) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(comparator, "comparator");  // arg

        int sizeList = values.size();

        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        Thread[] pool = new Thread[threads];

        int[] result = new int[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[numberThread] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;

                int indexMinValue = start;
                T minValue = values.get(start);

                for (int j = start; j < indexEnd; j += step) {
                    if (comparator.compare(values.get(j), minValue) < 0) {
                        indexMinValue = j;
                        minValue = values.get(j);
                    }
                }

                result[numberThread] = indexMinValue;
            });


            pool[k].start();
        }

        // ждем каждый поток
        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        // считаем самый мин
        int indexGlobalMinValues = result[0];
        T globalMinValue = values.get(result[0]);
        for (int i : result) {
            if (comparator.compare(values.get(i), globalMinValue) < 0) {
                indexGlobalMinValues = i;
                globalMinValue = values.get(i);
            }
        }

        return indexGlobalMinValues;
    }


    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate, int step) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        int[] result = new int[threads];
        Arrays.fill(result, Integer.MAX_VALUE);
        Thread[] pool = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[k] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;
                for (int j = start; j < indexEnd; j += step) {
                    if (predicate.test(values.get(j))) {
                        result[numberThread] = j;
                        break;
                    }
                }
            });

            pool[k].start();
        }

        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }
            throw e;
        }

        int indexMin = Integer.MAX_VALUE;
        for (int i : result) {
            if (i < indexMin)
                indexMin = i;
        }

        return indexMin < Integer.MAX_VALUE ? indexMin : -1;
    }


    @Override
    public <T> int lastIndexOf(int threads, List<T> values, Predicate<? super T> predicate, int step) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argMax/argMin
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;


        AtomicInteger resultIndex = new AtomicInteger(-1);
        Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;

            root[k] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;
                for (int j = start; j < indexEnd; j += step) {
                    if (predicate.test(values.get(j))) {
                        synchronized (resultIndex) {
                            if (j > resultIndex.get())
                                resultIndex.set(j);
                        }
                    }
                }
            });

            root[k].start();
        }

        try {
            for (Thread t : root) {
                if (t != null) {
                    t.join();
                }
            }
        } catch (InterruptedException e) {
            for (Thread t : root) {
                if (t != null) {
                    t.interrupt();
                }
            }

            throw e;
        }

        return resultIndex.get();
    }


    @Override
    public <T> long sumIndices(int threads, List<? extends T> values, Predicate<? super T> predicate, int step) throws InterruptedException {

        Objects.requireNonNull(values,     "values");      // argices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // ind

        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;


        AtomicLong resultIndex = new AtomicLong(0);
        Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart = k * chunkSize;
            final int indexEnd = Math.min(indexStart + chunkSize, sizeList);

            if (indexStart >= indexEnd) break;


            root[k] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;
                for (int j = start; j < indexEnd; j += step) {
                    if (predicate.test(values.get(j))) {
                        resultIndex.addAndGet(j);
                    }
                }
            });

            root[k].start();
        }


        try {
            for (Thread t : root) {
                if (t != null) {
                    t.join();
                }
            }
        } catch (InterruptedException e) {
            for (Thread t : root) {
                if (t != null) {
                    t.interrupt();
                }
            }

            throw e;
        }


        return resultIndex.get();
    }

    @Override
    public <T> int[] indices(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = list.size();

        i = Math.min(i, sizeList);
        if (sizeList == 0) return new int[0];
        if (i < 1) return new int[0];

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        int[][] result = new int[i][];
        int[] counts = new int[i];
        Throwable[] errors = new Throwable[i];

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;

            if (indexStart >= indexEnd) break;
            final int numberThread = k;


            pool[k] = new Thread(() -> {
                try {
                    int[] matches = new int[indexEnd - indexStart];
                    int count = 0;

                    for (int j = indexStart; j < indexEnd; j++) {
                        if (predicate.test(list.get(j))) {
                            matches[count++] = j;
                        }
                    }

                    result[numberThread] = matches;
                    counts[numberThread] = count;
                } catch (Throwable t) {
                    errors[numberThread] = t;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }

        for (Throwable t : errors) {
            if (t == null) continue;
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof Error e) throw e;
        }


        int total = 0;
        for (int count : counts)
            total += count;

        int[] globalResult = new int[total];
        int offset = 0;

        for (int k = 0; k < i; k++) {
            System.arraycopy(
                    result[k], 0,
                    globalResult, offset,
                    counts[k]
            );
            offset += counts[k];
        }

        return globalResult;
    }


    @Override
    public <T> List<T> filter(int i, List<? extends T> list, Predicate<? super T> predicate) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // indexOf/lastIndexOf/sumIndices/indices/filter

        int sizeList = list.size();

        if (sizeList == 0 || i < 1) return new ArrayList<>();
        i = Math.min(i, sizeList);

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        Throwable[] error = new Throwable[i];
        List<List<T>> results = new ArrayList<>();



        for (int k = 0; k < i; k++) {
            results.add(new ArrayList<>());
        }

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;
            final int numberThread = k;


            pool[k] = new Thread(() -> {
                try {
                    List<T> matches = results.get(numberThread);

                    for (int j = indexStart; j < indexEnd; j++) {
                        T value = list.get(j);
                        if (predicate.test(value)) {
                            matches.add(value);
                        }
                    }
                } catch (Throwable t) {
                    error[numberThread] = t;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }


        for (Throwable t : error) {
            if (t == null) continue;
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof Error e) throw e;
        }


        return results.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }


    @Override
    public <T, U> List<U> map(int i, List<? extends T> list, Function<? super T, ? extends U> function) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indexOf/lastIndexOf/sumIndices/indices/filter
        Objects.requireNonNull(function,   "function");    // map

        int sizeList = list.size();

        if (sizeList == 0 || i < 1) return new ArrayList<>();
        i = Math.min(i, sizeList);

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        Throwable[] error = new Throwable[i];

        List<List<U>> results = new ArrayList<>();

        for (int k = 0; k < i; k++) {
            results.add(new ArrayList<>());
        }

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;
            final int numberThread = k;


            pool[k] = new Thread(() -> {
                try {
                    List<U> matches = IntStream.range(indexStart, indexEnd)
                            .mapToObj(list::get)
                            .map(function)
                            .collect(Collectors.toList());

                    results.get(numberThread).addAll(matches);
                } catch (Throwable e) {
                    error[numberThread] = e;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }



        for (Throwable t : error) {
            switch (t) {
                case null -> {
                    continue;
                }
                case RuntimeException re -> throw re;
                case Error e -> throw e;
                default -> {
                }
            }
        }


        return results.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }


















    @Override
    public <T> int[] indices(int i, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // ind

        int sizeList = list.size();

        if (sizeList == 0) return new int[0];
        i = Math.min(i, sizeList);
        if (i < 1) return new int[0];

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        Throwable[] error = new Throwable[i];
        int[][] result = new int[i][];
        int[] counts = new int[i];

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;
            final int numberThread = k;

            if (indexStart >= indexEnd) break;


            pool[k] = new Thread(() -> {
                try {
                    int start = ((indexStart + step - 1) / step) * step;

                    int[] matches = new int[indexEnd - indexStart];
                    int count = 0;

                    for (int j = start; j < indexEnd; j += step) {
                        if (predicate.test(list.get(j))) {
                            matches[count++] = j;
                        }
                    }

                    result[numberThread] = matches;
                    counts[numberThread] = count;
                } catch (Throwable e) {
                    error[numberThread] = e;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }

        for (Throwable t : error) {
            switch (t) {
                case null -> {
                    continue;
                }
                case RuntimeException re -> throw re;
                case Error e -> throw e;
                default -> {}
            }
        }


        int total = 0;
        for (int count : counts)
            total += count;

        int[] globalResult = new int[total];
        int offset = 0;

        for (int k = 0; k < i; k++) {
            System.arraycopy(
                    result[k], 0,
                    globalResult, offset,
                    counts[k]
            );
            offset += counts[k];
        }

        return globalResult;
    }











    @Override
    public <T> List<T> filter(int i, List<? extends T> list, Predicate<? super T> predicate, int step) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(predicate,  "predicate");   // ind

        int sizeList = list.size();

        if (sizeList == 0 || i < 1) return new ArrayList<>();
        i = Math.min(i, sizeList);

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        Throwable[] error = new Throwable[i];

        List<List<T>> results = new ArrayList<>();

        for (int k = 0; k < i; k++) {
            results.add(new ArrayList<>());
        }

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;
            final int numberThread = k;


            pool[k] = new Thread(() -> {
                try {
                    int start = ((indexStart + step - 1) / step) * step;

                    List<T> matches = results.get(numberThread);

                    for (int j = start; j < indexEnd; j += step) {
                        T value = list.get(j);
                        if (predicate.test(value)) {
                            matches.add(value);
                        }
                    }
                } catch (Throwable t) {
                    error[numberThread] = t;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }



        for (Throwable t : error) {
            switch (t) {
                case null -> { continue; }
                case RuntimeException re -> throw re;
                case Error e -> throw e;
                default -> throw new IllegalStateException("Unexpected value: " + t);
            }
        }


        return results.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public <T, R> List<R> map(int i, List<? extends T> list, Function<? super T, ? extends R> function, int step) throws InterruptedException {

        Objects.requireNonNull(list,       "list");        // indices/filter/map/indexOf/lastIndexOf/sumIndices
        Objects.requireNonNull(function,   "function");    // map




        int sizeList = list.size();

        if (sizeList == 0 || i < 1) return new ArrayList<>();
        i = Math.min(i, sizeList);

        int chunkSize = (sizeList + i - 1) / i;


        Thread[] pool = new Thread[i];
        Throwable[] error = new Throwable[i];

        List<List<R>> results = new ArrayList<>();

        for (int k = 0; k < i; k++) {
            results.add(new ArrayList<>());
        }

        for (int k = 0; k < i; k++) {

            final int indexStart = k * sizeList / i;
            final int indexEnd = (k + 1) * sizeList / i;
            final int numberThread = k;


            pool[k] = new Thread(() -> {
                try {
                    int start = ((indexStart + step - 1) / step) * step;

                    List<R> matches = IntStream.iterate(start, x -> x < indexEnd, x -> x + step)
                            .mapToObj(list::get)
                            .map(function)
                            .collect(Collectors.toList());

                    results.get(numberThread).addAll(matches);
                } catch (Throwable t) {
                    error[numberThread] = t;
                }
            });

            pool[k].start();
        }


        try {
            for (Thread t : pool) {
                if (t != null)
                    t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : pool) {
                if (t != null)
                    t.interrupt();
            }

            throw e;
        }



        for (Throwable t : error) {
            switch (t) {
                case null -> { continue; }
                case RuntimeException re -> throw re;
                case Error e -> throw e;
                default -> throw new InterruptedException("Unexpected value: " + t);
            }
        }

        return results.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}