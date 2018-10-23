import sys

class TopKSort(object):
    """
    从数组中寻找到最大的K个值
    数组和K值存放在指定文件中
    """

    def __init__(self, file_name):
        self.K, self.data = self.read_file(file_name)
        self.K = int(self.K)
        self.data = list(map(lambda x: float(x.strip()), self.data))
        self.top_k = self.find_top_k(self.data, self.K)
        # print(self.top_k)
        self.quick_sort(self.top_k, 0, self.K - 1)
        self.format_output()

    def find_top_k(self, data, k):
        """
        找到最大的K个数字,未排序
        :param data: 需要查找的数组
        :param k:指定的K个值
        :return: data 中最大的k个数组
        """
        if len(data) < k:
            return data
        pivot = data[-1]
        bigger_than_pivot = [pivot]
        bigger_than_pivot.extend(list(filter(lambda x: x >= pivot, data[:-1])))
        bigger_than_pivot_len = len(bigger_than_pivot)
        if bigger_than_pivot_len == k:
            return bigger_than_pivot
        if bigger_than_pivot_len > k:
            return self.find_top_k(bigger_than_pivot, k)
        else:
            small_than_pivot = list(filter(lambda x: x < pivot, data[:-1]))
            return self.find_top_k(small_than_pivot, k - bigger_than_pivot_len) + bigger_than_pivot

    def read_file(self, file_name):
        """
        Read data from specific file
        :param file_name: 存放数据的文件名
        :return: K， data
        """
        with open(file_name, 'r', encoding='utf-8') as r:
            data = r.readlines()
        return data[0], data[1:]

    def quick_sort(self, top_k, left, right):
        """
        快速排序
        :param top_k: 需要排序的数组
        :param left:数组的下界
        :param right:数组的上界
        :return:(原址排序，无返回值)排序后的top_k
        """
        if left < right:
            split_line = self.partition(top_k, left, right)
            self.quick_sort(top_k, left, split_line - 1)
            self.quick_sort(top_k, split_line + 1, right)

    def partition(self, top_k, left, right):
        """
        对传入的数组进行切片
        :param top_k: 传入的数组
        :param left:
        :param right:
        :return: 切片的下标
        """
        pivot = top_k[right]
        i = left - 1
        for j in range(left, right):
            if top_k[j] >= pivot:
                i += 1
                top_k[i], top_k[j] = top_k[j], top_k[i]
        top_k[i + 1], top_k[right] = top_k[right], top_k[i + 1]
        return i + 1

    def format_output(self):
        data_str = list(map(lambda x: str(x), self.top_k))
        print(",".join(data_str), end='')


if __name__ == '__main__':
    file = sys.argv[1]
    sort = TopKSort(file)
