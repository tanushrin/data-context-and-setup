import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities
        #pass  # YOUR CODE HERE
        #csv_path = os.path.join(os.getcwd(), '../data-context-and-setup/data/csv')
        #print(f'csv_path is: {csv_path}'  )
        csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data-context-and-setup/data/csv'))
        #print(f'csv_path is: {csv_path}'  )
        file_names = os.listdir(csv_path)
        file_names = [f for f in file_names if f.endswith('.csv')]
        #print(file_names)
        data = {}
        data_frames = [pd.read_csv(os.path.join(csv_path, f)) for f in file_names]
        key_names = [f.replace('.csv', '').replace('_dataset', '').replace('olist_', '') for f in file_names]
        for (x, y) in zip(key_names, data_frames):
            data[x] = y

        #print(data)
        return data


    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
