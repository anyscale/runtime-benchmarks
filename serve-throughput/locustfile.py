from locust import HttpUser, task, constant
import random


class DLRMUser(HttpUser):
    wait_time = constant(0)
    auth_token = "<INSERT AUTH TOKEN HERE>"
    
    def on_start(self):
        self.client.headers.update({
            "Authorization": f"Bearer {self.auth_token}", 
            "x-request-disconnect-disabled": "?1" 
        })
    
    @task
    def get_recommendations(self):
        user_id = random.randint(1, 2)
        with self.client.get(
            "/",
            params={"user_id": user_id},
            catch_response=True
        ) as response:
            if response.status_code == 200:
                try:
                    json_response = response.json()
                    if "scores" in json_response:
                        response.success()
                    else:
                        response.failure("Response missing 'scores' field")
                except Exception as e:
                    response.failure(f"Failed to parse JSON: {e}")
            else:
                response.failure(f"Got status code {response.status_code}")
