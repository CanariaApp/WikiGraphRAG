import time
from tqdm import tqdm

class ProgressIndicator:
    def __init__(self, seconds_between_updates):
        self.page_count = 0
        self.last_page_count = 0
        self.last_update_time = self.start_time = time.time()
        self.seconds_between_updates = seconds_between_updates
        self.pbar = tqdm()

    def on_element(self, _):
        self.page_count += 1
        now = time.time()

        if self.last_update_time + self.seconds_between_updates < now:
            self.display_updates()
            self.last_update_time = now
            self.last_page_count = self.page_count

    def display_updates(self):
        now = time.time()
        lifetime_speed = self.page_count / (now - self.start_time)
        momentary_speed = (self.page_count - self.last_page_count) / (
            now - self.last_update_time
        )

        self.pbar.set_description(
            f"Page Count: {self.page_count}, Pages per second: Global ({int(lifetime_speed)}) / Momentary ({int(momentary_speed)})"
        )
        self.pbar.update(self.page_count - self.pbar.n)
