
class PySorted(object):

    @classmethod
    def bubble(cls, array, desc=False):
        """
        Input an array to be sorted and output sorted array, asc
        """
        array_len = len(array)

        for i in range(array_len):
            for j in range(0, array_len - i):
                if array[j] > array[j + 1]:
                    array[j], array[j + 1] = array[j + 1], array[j]

        if desc:
            array = array[::-1]

        return array
