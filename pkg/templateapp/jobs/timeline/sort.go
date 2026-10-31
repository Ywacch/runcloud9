package timeline

import "sort"

func sortItems(items []Item) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].PostID > items[j].PostID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
}
