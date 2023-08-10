from flask import Flask, render_template, request
import json
import main_service
import time

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def main():
    if request.method == 'POST':
        value = int(request.form['input_value'])
        order_id = main_service.order_lottos(int(value))
        lotto_list = main_service.get_order(order_id)
        return render_template('main.html', data_list=lotto_list)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
