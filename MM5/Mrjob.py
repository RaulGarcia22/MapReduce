import time
from mrjob.job import MRJob
from mrjob.step import MRStep

class MatrixMultiplication(MRJob):

    def configure_args(self):
        super(MatrixMultiplication, self).configure_args()
        self.add_passthru_arg('--matrix-a-rows', type=int, help='Number of rows in matrix A')
        self.add_passthru_arg('--matrix-b-cols', type=int, help='Number of columns in matrix B')

    def mapper(self, _, line):
        matrix, row, col, value = line.split(',')
        row = int(row.strip())
        col = int(col)
        value = float(value)

        if matrix == 'A':
            for k in range(self.options.matrix_b_cols):
                yield (row, k), (matrix, col, value)
        else:
            for i in range(self.options.matrix_a_rows):
                yield (i, col), (matrix, row, value)

    def reducer(self, key, values):
        result = 0

        matrix_a_values = []
        matrix_b_values = []
        for value in values:
            matrix, index, val = value
            if matrix == 'A':
                matrix_a_values.append((index, val))
            else:
                matrix_b_values.append((index, val))

        for a in matrix_a_values:
            for b in matrix_b_values:
                if a[0] == b[0]:
                    result += a[1] * b[1]

        #yield key, result

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    start_time = time.time()
    MatrixMultiplication.run()
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")