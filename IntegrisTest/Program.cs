using System;
using System.Threading;
using System.Threading.Tasks;
using shopStoragedll;

namespace IntegrisTest
{
    class Program
    {
        static Random rnd = new Random();
        static int sum = 0;
        static shopStorage shopStorage;
        static void Main(string[] args)
        {
            shopStorage = new shopStorage("localhost", "postgres", "ghf2plyb3r", "usersdb");
            for (int i=0;i<10;i++)
                TestAsync(i);
            Console.Read();
            shopStorage.ShowReserves("Тестовый товар");
            

        }
        static async void TestAsync(int number)
        {
            int k = 0;
            k = await Task.Run(() => ReserveTask(number));
            sum += k;
            Console.WriteLine("Поток {0} ЗАВЕРШИЛ работу, сумма равна {1}", number, sum);
        }
        static int ReserveTask(int number)
        {
            Console.WriteLine("Начало потока {0}", number);
            int sum = 0;
            for (int i = 0; i < 1000; i++)
            {
                int count = rnd.Next(1, 4);
                int reserveresult = shopStorage.Reserve("Тестовый товар", count, number.ToString());
                if (reserveresult>0)
                {
                    sum += reserveresult;
                }
            }
            return sum;
        }
    }
}
