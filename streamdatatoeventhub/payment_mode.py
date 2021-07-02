from faker.providers import BaseProvider
import random
class PaymentMode(BaseProvider):
    def payment(self):
        payment_mode_arr = [ "Cash on Delivery", "Debit Card", "Credit Card", "Online Banking", "M-wallet"]
        return random.choice(payment_mode_arr)