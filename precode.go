package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	var i int64 = 1
	for {
		select {
		case <-ctx.Done():
			close(ch)
			return
		case ch <- i:
			fn(i)
			i++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	for v := range in {
		out <- v
		time.Sleep(time.Millisecond) // пауза на 1 миллисекунду
	}
	close(out)
}

func main() {
	chIn := make(chan int64)

	// Создаем контекст с таймаутом на 1 секунду
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// генерируем числа, считая параллельно их количество и сумму
	// Используем атомарные операции для избежания гонки данных
	go Generator(ctx, chIn, func(i int64) {
		atomic.AddInt64(&inputSum, i)
		atomic.AddInt64(&inputCount, 1)
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut)
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// 4. Собираем числа из каналов outs
	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		idx := i
		ch := outs[i]

		go func() {
			defer wg.Done()
			for v := range ch {
				amounts[idx]++
				chOut <- v
			}
		}()
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала
	for v := range chOut {
		count++
		sum += v
	}

	fmt.Println("Количество чисел", atomic.LoadInt64(&inputCount), count)
	fmt.Println("Сумма чисел", atomic.LoadInt64(&inputSum), sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if atomic.LoadInt64(&inputSum) != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", atomic.LoadInt64(&inputSum), sum)
	}
	if atomic.LoadInt64(&inputCount) != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", atomic.LoadInt64(&inputCount), count)
	}

	remaining := atomic.LoadInt64(&inputCount)
	for _, v := range amounts {
		remaining -= v
	}
	if remaining != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное, остаток: %d\n", remaining)
	}
}
