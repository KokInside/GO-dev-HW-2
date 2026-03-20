package main

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	chans := make([]chan interface{}, len(cmds)+1)
	for i := range len(chans) {
		chans[i] = make(chan interface{})
	}

	wg := sync.WaitGroup{}

	for i, c := range cmds {
		chan1, chan2 := chans[i], chans[i+1]

		wg.Add(1)
		go func(c cmd) {
			defer wg.Done()
			defer close(chan2)
			c(chan1, chan2)
		}(c)
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	selectedUsers := make(map[string]bool)
	wg := sync.WaitGroup{}
	mu := sync.RWMutex{}

	for emailItf := range in {
		email := emailItf.(string)

		if usersAliases[email] != "" {
			email = usersAliases[email]
		}

		wg.Add(1)
		go func(w *sync.WaitGroup, mu *sync.RWMutex) {
			defer wg.Done()
			user := GetUser(email)

			mu.RLock()
			alreadySelected := selectedUsers[user.Email]
			mu.RUnlock()

			if alreadySelected == false {
				out <- user
				mu.Lock()
				selectedUsers[user.Email] = true
				mu.Unlock()
			}
		}(&wg, &mu)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := sync.WaitGroup{}

	for {
		userItf, ok := <-in
		userSlice := make([]User, 0, 2)

		if ok {
			userSlice = append(userSlice, userItf.(User))
		} else {
			break
		}

		user2Itf, ok := <-in
		if ok {
			userSlice = append(userSlice, user2Itf.(User))
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			MsgID, err := GetMessages(userSlice...)
			if err != nil {
				return
			}

			for _, id := range MsgID {
				out <- id
			}
		}(&wg)

		if !ok {
			break
		}
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	queue := make(chan int, 5)
	wg := sync.WaitGroup{}

	for idItf := range in {
		queue <- 1

		id := idItf.(MsgID)
		wg.Add(1)
		go func(w *sync.WaitGroup) {
			defer w.Done()
			defer func() { <-queue }()

			res, err := HasSpam(id)
			if err != nil {
				return
			}

			out <- MsgData{ID: id, HasSpam: res}

		}(&wg)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var datas []MsgData

	for dataItf := range in {
		data := dataItf.(MsgData)
		datas = append(datas, data)
	}

	slices.SortFunc(datas, func(a, b MsgData) int {
		if a.HasSpam == b.HasSpam {
			return cmp.Compare(a.ID, b.ID)
		}
		if a.HasSpam == true {
			return -1
		}
		return 1
	})

	for _, data := range datas {
		out <- fmt.Sprintf("%v %v", data.HasSpam, data.ID)
	}
}
