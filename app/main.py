import sys
sys.path.extend('/vol/usr/src/ip_clean/biz:/vol/usr/src/ip_clean/proj:/vol/usr/src/ip_clean'.split(':'))
from app import App

def main():
    app = App()
    app.run()

if __name__ == '__main__':
    main()
