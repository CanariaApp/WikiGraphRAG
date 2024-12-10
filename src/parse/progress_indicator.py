import time
from tqdm import tqdm

class ProgressIndicator:
    def __init__(self, seconds_between_updates, tot_page_number=2.4e7):
        self.page_count = 0
        self.last_page_count = 0
        self.last_update_time = self.start_time = time.time()
        self.seconds_between_updates = seconds_between_updates
        self.tot_page_number = int(tot_page_number)
        self.pbar = tqdm(total=self.tot_page_number)

    def on_element(self, _):
        self.page_count += 1
        now = time.time()

        if self.last_update_time + self.seconds_between_updates < now:
            self.display_updates()
            self.last_update_time = now
            self.last_page_count = self.page_count

    def display_updates(self):
        now = time.time()
        t = (now - self.start_time)
        lifetime_speed = self.page_count / t
        momentary_speed = (self.page_count - self.last_page_count) / (
            now - self.last_update_time
        )

        self.pbar.set_description(f"Page Count: {self.page_count}")# Estimated time left {(self.tot_page_number/self.page_count-1)*t/3600} hrs")
        self.pbar.update(self.page_count - self.pbar.n)
