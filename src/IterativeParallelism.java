import info.kgeorgiy.java.advanced.iterative.NewScalarIP;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.Comparator;
import java.util.List;

public class IterativeParallelism implements NewScalarIP {


    @Override
    public <T> int argMax(int threads, List<T> values, Comparator<? super T> comparator) throws InterruptedException {
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
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);

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
        int sizeList = values.size();

        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        Thread[] pool = new Thread[threads];

        int[] result = new int[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[numberThread] = new Thread(() -> {
                int indexMinValue = indexStart;
                T minValue = values.get(indexStart);

                for (int j = indexStart + 1; j < indexEnd; j++) {
                    if (comparator.compare(values.get(j), minValue) < 0) {
                        indexMinValue = j;
                        minValue      = values.get(j);
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
                globalMinValue       = values.get(i);
            }
        }

        return indexGlobalMinValues;
    }

    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate) throws InterruptedException {
        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        int[] result = new int[threads];
        Arrays.fill(result, Integer.MAX_VALUE);
        Thread[] pool = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);
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
        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;



       AtomicInteger resultIndex = new AtomicInteger(-1);
       Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);

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
        } catch(InterruptedException e) {
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
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);

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
        int sizeList = values.size();

        if (sizeList == 0) throw new NoSuchElementException("List<T> is Empty");
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        Thread[] pool = new Thread[threads];

        int[] result = new int[threads];
        for (int k = 0; k < threads; k++) {

            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);
            final int numberThread = k;

            if (indexStart >= indexEnd) break;

            pool[numberThread] = new Thread(() -> {
                int start = ((indexStart + step - 1) / step) * step;

                int indexMinValue = start;
                T minValue = values.get(start);

                for (int j =  start; j < indexEnd; j += step) {
                    if (comparator.compare(values.get(j), minValue) < 0) {
                        indexMinValue = j;
                        minValue      = values.get(j);
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
                globalMinValue       = values.get(i);
            }
        }

        return indexGlobalMinValues;
    }



    @Override
    public <T> int indexOf(int threads, List<T> values, Predicate<? super T> predicate, int step) throws InterruptedException {
        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;

        int[] result = new int[threads];
        Arrays.fill(result, Integer.MAX_VALUE);
        Thread[] pool = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);
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
        int sizeList = values.size();

        if (sizeList == 0) return -1;
        threads = Math.min(threads, sizeList);
        if (threads < 1) return -1;

        int chunkSize = (sizeList + threads - 1) / threads;



        AtomicInteger resultIndex = new AtomicInteger(-1);
        Thread[] root = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int indexStart   = k * chunkSize;
            final int indexEnd     = Math.min(indexStart + chunkSize, sizeList);

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
        } catch(InterruptedException e) {
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

}