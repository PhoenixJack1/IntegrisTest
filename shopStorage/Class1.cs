using System;
using Npgsql;

namespace shopStoragedll
{
    public class shopStorage
    {
        string connectionstring;
        /// <summary> создаёт объект доступа к базе данных PostreSQL, а также тестовые таблицы с данными и процедуру в БД </summary>
        public shopStorage(string host, string username, string password, string database)
        {
            connectionstring = String.Format("Host={0};Username={1};Password={2};Database={3}", host, username, password, database);
            AddOrderFunction();
            CreateGoodsTable();
            CreateOrdersTable();

            Console.WriteLine("База создана");
        }
        /// <summary> Показывает список резервирований товара, его количество и остаток </summary>
        /// <param name="name">Наименование товара</param>
        public void ShowReserves(string name)
        {
            Console.WriteLine("Результаты резервирования товара: " + name);
            using (NpgsqlConnection connection = new NpgsqlConnection(connectionstring))
            {
                string query = @"SELECT orders.customer_phone, COUNT (orders.count), SUM(orders.count) 
                                FROM orders JOIN goods ON orders.good_id=goods.id 
                                WHERE goods.name='" + name + "' GROUP BY orders.customer_phone;";
                NpgsqlCommand command = new NpgsqlCommand(query, connection);
                connection.Open();
                NpgsqlDataReader reader = command.ExecuteReader();
                Console.WriteLine("Номер телефона заказчика\tКоличество резервирований\tКоличество товара");
                while (reader.Read())
                {
                    Console.WriteLine("{0}\t\t\t\t{1}\t\t\t\t{2}", reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2));
                }
                connection.Close();
                query = "SELECT SUM(orders.count), goods.store, (goods.store-SUM(orders.count)) FROM orders JOIN goods ON orders.good_id=goods.id WHERE goods.name='" + name + "' GROUP BY goods.id;";
                command = new NpgsqlCommand(query, connection);
                connection.Open();
                reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Console.WriteLine("Всего товара: " + reader.GetInt32(1).ToString());
                    Console.WriteLine("Зарезервировано товара: " + reader.GetInt32(0).ToString());
                    Console.WriteLine("Остаток товара: " + reader.GetInt32(2));
                }
                connection.Close();
             }
        }
        string GetDatePostreSQL(DateTime time)
        {
            return string.Format("{0}-{1:00}-{2:00} {3:00}:{4:00}:{5:00}", time.Year, time.Month, time.Day, time.Hour, time.Minute, time.Second);
        }
        /// <summary> Резервирует товар со склада. Если остаток товара оказался меньше требуемого количества - резервирует остаток</summary>
        /// <param name="name">Наименование товара (не более 30 знаков)</param>
        /// <param name="val">Количество резервируемого товара</param>
        /// <param name="phone">Номер телефона заказчика (не более 20 знаков)</param>
        /// <returns>Возвращает количество зарезервированного товара. </returns>
        public int Reserve(string name, int val, string phone)
        {
            if (val <= 0) return 0;
            if (name.Length > 30) return 0;
            if (phone.Length > 20) return 0;
            int orders;
            lock (connectionstring)
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(connectionstring))
                {
                    string query = string.Format("SELECT add_order('{0}', {1}, '{2}', '{3}');", name, val, GetDatePostreSQL(DateTime.Now), phone);
                    NpgsqlCommand command = new NpgsqlCommand(query, connection);
                    connection.Open();
                    NpgsqlDataReader reader = command.ExecuteReader();
                    reader.Read();
                    orders = reader.GetInt32(0);
                    connection.Close();
                }
            }
            System.Threading.Thread.Sleep(1);
            return orders;
        }
        void CreateGoodsTable()
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(connectionstring))
            {
                string droptable = "DROP TABLE IF EXISTS goods CASCADE;";
                NpgsqlCommand command = new NpgsqlCommand(droptable, connection);
                connection.Open();
                command.ExecuteNonQuery();
                string createquery = "CREATE TABLE IF NOT EXISTS goods(id SERIAL PRIMARY KEY, name VARCHAR(30), price MONEY, store INTEGER NOT NULL);";
                command = new NpgsqlCommand(createquery, connection);
                command.ExecuteNonQuery();
                string addquery = "INSERT INTO goods (name, store) VALUES('Тестовый товар',100);";
                command = new NpgsqlCommand(addquery, connection);
                command.ExecuteNonQuery();
                connection.Close();

            }
        }
        void CreateOrdersTable()
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(connectionstring))
            {
                string droptable = "DROP TABLE IF EXISTS orders;";
                NpgsqlCommand command = new NpgsqlCommand(droptable, connection);
                connection.Open();
                command.ExecuteNonQuery();
                string createquery = "CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, good_id INTEGER REFERENCES goods(id), time TIMESTAMP, count INTEGER NOT NULL, customer_phone VARCHAR(20));";
                command = new NpgsqlCommand(createquery, connection);
                command.ExecuteNonQuery();
                connection.Close();
            }
        }
        void AddOrderFunction()
        {
            string query = @"CREATE OR REPLACE FUNCTION add_order(good_name text, val int, add_time TIMESTAMP, phone text) RETURNS int AS $$
                            DECLARE
	                            remains int;
	                            g_id int;
                            BEGIN
								SELECT id INTO g_id FROM goods WHERE name=good_name;
								IF g_id IS NULL THEN
									RETURN 0;
								END IF;								
                                SELECT (goods.store-SUM(orders.count)), goods.id INTO remains 
	                            FROM orders JOIN goods ON orders.good_id=goods.id
	                            WHERE goods.name=good_name GROUP BY goods.id;
								IF remains IS NULL THEN
									SELECT store INTO remains FROM goods WHERE id=g_id;
								END IF;
	                            IF remains>=val THEN
		                            INSERT INTO orders (good_id, time, count, customer_phone) VALUES (g_id, add_time, val, phone);
								    RETURN val;
	                            END IF;
	                            IF remains>0 THEN
		                            INSERT INTO orders (good_id, time, count, customer_phone) VALUES (g_id, add_time, remains, phone);
									RETURN remains;
	                            END IF;
		                            RETURN 0;
                                END;
                            $$ LANGUAGE plpgsql;";
            using (NpgsqlConnection connection = new NpgsqlConnection(connectionstring))
            {
                NpgsqlCommand command = new NpgsqlCommand(query, connection);
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }
    }
}
