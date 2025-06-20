import cmd
import sqlite3
import sqlparse
import os
import pandas as pd
import re
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'SimHei' 

class MyCommandLineTool(cmd.Cmd):
    prompt = '> '  # 命令行提示符

    def __init__(self):
        super().__init__()
        print("欢迎使用命令行工具！")
        self.db_file = os.path.join(os.getcwd(), 'project2025.db')

        # 在初始化时检测数据库是否存在
        if not os.path.exists(self.db_file):
            print("数据库project2025不存在，正在创建...")
            self.db_connection = self.reset_system()  # 仅在不存在时调用初始化
        else:
            print("数据库project2025已存在，直接连接")
            self.db_connection = sqlite3.connect(self.db_file)
        
        # 初始化插入模式相关属性
        self.insert_table = None
        self.insert_values = []
        self.in_insert_mode = False

    def reset_system(self):
        """创建数据库并初始化"""
        try:
            conn = sqlite3.connect(self.db_file)
            print("数据库project2025已创建")

            cursor = conn.cursor()

            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':
                    try:
                        # 删除表
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception as e:
                        print(f"表 {table_name} 无法删除，错误信息: {e}")

            conn.commit()
            return conn
        except Exception as e:
            print(f"系统初始化失败: {e}")
            return None

    def do_cls(self, arg):
        """清屏函数"""
        os.system('cls' if os.name == 'nt' else 'clear')
        return None

    def check_sql(self, arg):
        
        pass
    
    def execute_sql(self, arg):
        """执行 SQL 语句"""
        if self.db_connection is None:
            print("数据库连接失败，无法执行SQL语句")
            return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(arg)
            if arg.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                self.print_table(results, [desc[0] for desc in cursor.description])
            else:
                self.db_connection.commit()
                print("SQL 语句执行成功")
        except sqlite3.OperationalError as e:
            error_message = str(e)
            if "no such table" in error_message:
                table_name = error_message.split("no such table: ")[1].strip()
                print(f"错误：表 {table_name} 不存在。请检查表名是否正确，或者先创建该表。")
            elif "near" in error_message:
                position = error_message.split("near ")[1].split(":")[0].strip()
                print(f"错误：在 {position} 附近存在语法错误。请检查该位置的语法，可能是关键字拼写错误、缺少逗号或括号不匹配等。")
            else:
                print(f"执行 SQL 语句时出错: {e}")
        except Exception as e:
            print(f"执行 SQL 语句时出错: {e}")

    def do_list_tables(self, arg):
        """列出数据库中的表并显示表的数量"""
        if self.db_connection is None:
            print("数据库连接失败，无法列出表")
            return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            print(f"数据库中共有 {len(tables)} 个表，表名如下:")
            for table in tables:
                print(table[0])
        except Exception as e:
            print(f"获取表信息时出错: {e}")

    def do_create_table(self, arg):
        """添加新表，参数格式: create_table 表名 (标准SQL列定义)"""
        # 分割表名和列定义
        try:
            table_name, columns_part = arg.strip().split(' ', 1)
            table_name = table_name.strip()
            columns_part = columns_part.strip()

            # 验证括号格式
            if not columns_part.startswith('(') or not columns_part.endswith(')'):
                print("列定义必须用括号包围，格式如: create_table my_table (id INTEGER, name TEXT)")
                return

            # 提取括号内的列定义
            columns_def = columns_part[1:-1].strip()
            if not columns_def:
                print("列定义不能为空")
                return

            # 构建完整的CREATE TABLE语句
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"

            # 执行SQL
            cursor = self.db_connection.cursor()
            self.check_sql(sql)  # 检查SQL语法
            cursor.execute(sql)
            self.db_connection.commit()
            print(f"表 {table_name} 创建成功")

        except ValueError:
            print("参数格式错误，格式如: add_table my_table (id INTEGER, name TEXT)")
        except sqlite3.OperationalError as e:
            print(f"创建表失败: {e}")
        except Exception as e:
            print(f"发生未知错误: {e}")

    def do_insert_into(self, arg):
        """执行INSERT INTO... VALUES语句"""
        if self.db_connection is None:
            print("数据库连接失败，无法执行插入操作")
            return

        if self.in_insert_mode:
            # 处理插入值或结束插入
            if arg.lower() == ';':
                if self.insert_table and self.insert_values:
                    values_str = ', '.join([f"({value})" for value in self.insert_values])
                    sql = f"INSERT INTO {self.insert_table} VALUES {values_str};"
                    try:
                        cursor = self.db_connection.cursor()
                        self.check_sql(sql)
                        cursor.execute(sql)
                        self.db_connection.commit()
                        print("插入操作成功")
                    except sqlite3.IntegrityError as e:
                        if "UNIQUE constraint failed" in str(e):
                            cursor.execute(f"PRAGMA table_info({self.insert_table})")
                            columns = cursor.fetchall()
                            primary_key_column = next((col[1] for col in columns if col[5] == 1), None)
                            if primary_key_column:
                                print(f"插入错误：主键列 {primary_key_column} 的值重复，请检查输入。")
                            else:
                                print("插入错误：主键值重复，请检查输入。")
                        else:
                            print(f"执行插入操作时出错: {e}")
                    except sqlite3.OperationalError as e:
                        error_message = str(e)
                        if "no such table" in error_message:
                            print(f"错误：表 {self.insert_table} 不存在。请检查表名是否正确，或者先创建该表。")
                        elif "near" in error_message:
                            position = error_message.split("near ")[1].split(":")[0].strip()
                            print(f"错误：在 {position} 附近存在语法错误。请检查该位置的语法。")
                        else:
                            print(f"执行插入操作时出错: {e}")
                    finally:
                        self.reset_insert_state()
                else:
                    print("没有有效的插入数据，请重新开始。")
                    self.reset_insert_state()
                return
            else:
                # 验证插入值的格式
                if "," not in arg:
                    print("输入的值格式错误，请确保值之间用逗号分隔")
                    return
                self.insert_values.append(arg)
                print(f"已添加值: {arg}")
        else:
            # 开始新的插入流程
            if not arg:
                print("请指定表名，格式为: insert_into <表名>")
                return

            table_name = arg
            try:
                cursor = self.db_connection.cursor()
                # 检查表名是否存在（不区分大小写）
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0].lower() for row in cursor.fetchall()]
                if table_name.lower() not in tables:
                    print(f"表 {table_name} 不存在，请先创建该表")
                    return

                # 获取实际表名（使用正确的大小写）
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND LOWER(name)=?", (table_name.lower(),))
                actual_table_name = cursor.fetchone()[0]
                table_name = actual_table_name
            except sqlite3.Error as e:
                print(f"检查表是否存在时出错: {e}")
                return

            # 获取表结构并显示列信息
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()

                # 提取列名和数据类型
                column_names = [col[1] for col in columns]
                column_types = [col[2] for col in columns]

                # 计算每列的最大宽度（列名和类型的最大长度）
                max_widths = []
                for i in range(len(column_names)):
                    max_name_width = len(column_names[i])
                    max_type_width = len(column_types[i])
                    max_widths.append(max(max_name_width, max_type_width))

                # 构建表头分隔线
                print("userinfo表结构为:")

                separator = "-" * (sum(max_widths) + 3 * (len(column_names) - 1))

                # 构建列名行
                name_line = "   ".join([name.ljust(width) for name, width in zip(column_names, max_widths)])

                # 构建类型行
                type_line = "   ".join([typ.ljust(width) for typ, width in zip(column_types, max_widths)])

                # 打印表结构
                print(separator)
                print(name_line)
                print(type_line)
                print(separator)

            except Exception as e:
                print(f"获取表结构时出错: {e}")

            self.insert_table = table_name
            self.insert_values = []
            self.in_insert_mode = True
            print("接下来请逐行输入要插入的数据值，每行输入对应一条完整记录，输入 ';' 结束插入。")
            self.prompt = f"{table_name}> "

    def reset_insert_state(self):
        """重置插入状态"""
        self.insert_table = None
        self.insert_values = []
        self.in_insert_mode = False
        self.prompt = '> '

    def do_desc_table(self, arg):
        """描述表的结构"""
        if self.db_connection is None:
            print("数据库连接失败，无法描述表结构")
            return
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({arg});")
            columns = cursor.fetchall()

            # 提取列名和数据类型
            column_names = [col[1] for col in columns]
            column_types = [col[2] for col in columns]

            # 计算每列的最大宽度（列名和类型的最大长度）
            max_widths = []
            for i in range(len(column_names)):
                max_name_width = len(column_names[i])
                max_type_width = len(column_types[i])
                max_widths.append(max(max_name_width, max_type_width))

            # 构建表头分隔线
            separator = "-" * (sum(max_widths) + 3 * (len(column_names) - 1))

            # 构建列名行
            name_line = "   ".join([name.ljust(width) for name, width in zip(column_names, max_widths)])

            # 构建类型行
            type_line = "   ".join([typ.ljust(width) for typ, width in zip(column_types, max_widths)])

            print(f"{arg}表结构为: ")
            print(separator)
            print(name_line)
            print(type_line)
            print(separator)

        except Exception as e:
            print(f"获取表结构时出错: {e}")

    def do_show_table_data(self, arg):
        """显示表中的具体信息"""
        if self.db_connection is None:
            print("数据库连接失败，无法显示表数据")
            return
        try:
            # 获取列名
            cursor = self.db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({arg});")
            columns = cursor.fetchall()
            column_names = [column[1] for column in columns]

            # 打印分隔线
            print("-" * 120)

            # 打印列名
            formatted_column_names = []
            for col_name in column_names:
                formatted_col_name = f"{col_name.ljust(20)} |"
                formatted_column_names.append(formatted_col_name)
            print("".join(formatted_column_names))

            # 获取表数据
            sql = f"SELECT * FROM {arg}"
            self.check_sql(sql)
            cursor.execute(sql)
            results = cursor.fetchall()

            # 打印数据
            for row in results:
                formatted_row = []
                for value in row:
                    formatted_value = f"{str(value).ljust(20)} |"
                    formatted_row.append(formatted_value)
                print("".join(formatted_row))

            print("-" * 120)
        except Exception as e:
            print(f"获取表 {arg} 数据时出错: {e}")

    def do_rename_column(self, arg):
        """
        重命名表中的列
        用法: rename_column <表名> <旧列名> <新列名>
        """
        if self.db_connection is None:
            print("数据库连接失败，无法执行列重命名操作")
            return

        args = arg.strip().split()
        if len(args) != 3:
            print("参数错误，用法: rename_column <表名> <旧列名> <新列名>")
            return

        table_name, old_col, new_col = args

        try:
            # 检查表是否存在
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                print(f"表 {table_name} 不存在，请先创建该表")
                return

            # 获取表的当前结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            # 检查旧列名是否存在
            if not any(col[1] == old_col for col in columns):
                print(f"列 {old_col} 不存在于表 {table_name} 中")
                return

            # 生成新的表结构定义
            new_columns = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                col_null = "NOT NULL" if col[3] == 1 else ""
                col_default = f"DEFAULT {col[4]}" if col[4] is not None else ""
                col_pk = "PRIMARY KEY" if col[5] == 1 else ""

                if col_name == old_col:
                    col_name = new_col

                parts = [col_name, col_type, col_null, col_default, col_pk]
                parts = [p for p in parts if p]  # 过滤空字符串
                new_columns.append(" ".join(parts))

            new_table_def = ", ".join(new_columns)

            # 开始事务以确保原子性
            self.db_connection.execute("BEGIN TRANSACTION")

            try:
                # 创建临时表
                temp_table = f"{table_name}_temp"
                cursor.execute(f"CREATE TABLE {temp_table} ({new_table_def})")

                # 复制数据
                columns_str = ", ".join([col[1] if col[1] != old_col else new_col for col in columns])
                old_columns_str = ", ".join([col[1] for col in columns])
                cursor.execute(f"INSERT INTO {temp_table} ({columns_str}) SELECT {old_columns_str} FROM {table_name}")

                # 删除原表
                cursor.execute(f"DROP TABLE {table_name}")

                # 重命名临时表
                cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

                # 提交事务
                self.db_connection.execute("COMMIT")
                print(f"列 {old_col} 已成功重命名为 {new_col}")

            except Exception as e:
                # 回滚事务
                self.db_connection.execute("ROLLBACK")
                raise e

        except sqlite3.Error as e:
            print(f"重命名列时出错: {e}")
        except Exception as e:
            print(f"执行列重命名操作时发生未知错误: {e}")
        
    def do_delete_table(self, arg):
        """删除指定的表"""
        if self.db_connection is None:
            print("数据库连接失败，无法删除表")
            return
        if arg == 'sqlite_sequence':
            print("不能删除 sqlite_sequence 表，请选择其他表进行删除操作。")
            return
        try:
            cursor = self.db_connection.cursor()
            sql = f"DROP TABLE IF EXISTS {arg}"
            self.check_sql(sql)
            cursor.execute(sql)
            self.db_connection.commit()
            print(f"表 {arg} 删除成功")
        except Exception as e:
            print(f"删除表 {arg} 时出错: {e}")

    def do_rename_table(self, arg):
        """重命名表，参数格式: 旧表名 新表名"""
        args = arg.split()
        if len(args) != 2:
            print("参数错误，用法: rename_table <旧表名> <新表名>")
            return
        old_table_name = args[0]
        new_table_name = args[1]

        try:
            sql = f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};"
            self.check_sql(sql)
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            self.db_connection.commit()
            print(f"表 {old_table_name} 已重命名为 {new_table_name}")
        except sqlite3.OperationalError as e:
            error_message = str(e)
            if "no such table" in error_message:
                print(f"错误：表 {old_table_name} 不存在。请检查表名是否正确。")
            else:
                print(f"重命名表时出错: {e}")
        except Exception as e:
            print(f"重命名表时出错: {e}")

    def do_truncate_table(self, arg):
        """清空指定表的数据，但不删除表结构"""
        if self.db_connection is None:
            print("数据库连接失败，无法清空表数据")
            return
        try:
            cursor = self.db_connection.cursor()
            sql = f"DELETE FROM {arg};"
            self.check_sql(sql)
            cursor.execute(sql)
            self.db_connection.commit()
            print(f"表 {arg} 数据已清空")
        except Exception as e:
            print(f"清空表 {arg} 数据时出错: {e}")

    def visualize_data(self, sql_query):
        """执行SQL查询并可视化结果"""
        if self.db_connection is None:
            print("数据库连接失败，无法执行查询操作")
            return
        
        try:
            # 执行SQL查询
            cursor = self.db_connection.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            
            # 获取列名
            column_names = [description[0] for description in cursor.description]
            
            # 转换为DataFrame以便处理
            df = pd.DataFrame(results, columns=column_names)
            
            # 数据可视化
            if len(df) > 0:
                # 自动检测适合可视化的列
                numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
                categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
                
                if not numeric_columns:
                    print("数据集中没有可用于可视化的数值列")
                    return
                
                # 选择合适的可视化方式
                if len(numeric_columns) >= 2 and len(df) >= 3:
                    # 如果有两个或以上数值列，且数据量足够，绘制散点图
                    plt.figure(figsize=(10, 6))  
                    plt.scatter(df[numeric_columns[0]], df[numeric_columns[1]])
                    plt.xlabel(numeric_columns[0])
                    plt.ylabel(numeric_columns[1])
                    plt.title(f"{numeric_columns[0]} vs {numeric_columns[1]}")
                    plt.grid(True)
                    plt.show()
                elif len(categorical_columns) >= 1 and len(numeric_columns) >= 1:
                    # 如果有分类列和数值列，绘制柱状图
                    plt.figure(figsize=(12, 6))  
                    if len(df[categorical_columns[0]].unique()) > 10:
                        # 如果分类太多，只显示前10个
                        top_categories = df[categorical_columns[0]].value_counts().index[:10]
                        df_filtered = df[df[categorical_columns[0]].isin(top_categories)]
                        df_grouped = df_filtered.groupby(categorical_columns[0])[numeric_columns[0]].mean().reset_index()
                    else:
                        df_grouped = df.groupby(categorical_columns[0])[numeric_columns[0]].mean().reset_index()
                    
                    plt.bar(df_grouped[categorical_columns[0]], df_grouped[numeric_columns[0]])
                    plt.xlabel(categorical_columns[0])
                    plt.ylabel(f"平均{numeric_columns[0]}")
                    plt.title(f"{numeric_columns[0]} 按 {categorical_columns[0]} 分组")
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    plt.show()
                else:
                    # 默认绘制数值列的直方图
                    plt.figure(figsize=(10, 6))  # 修改：使用 plt.figure()
                    plt.hist(df[numeric_columns[0]], bins=min(10, len(df)//2))
                    plt.xlabel(numeric_columns[0])
                    plt.ylabel("频数")
                    plt.title(f"{numeric_columns[0]} 的分布")
                    plt.grid(True)
                    plt.show()
            else:
                print("查询结果为空，无法进行可视化")
                
        except sqlite3.OperationalError as e:
            print(f"执行查询时出错: {e}")
        except Exception as e:
            print(f"可视化数据时出错: {e}")

    def default(self, line):
        """默认处理未识别的命令，优先尝试自然语言解析"""
        if self.in_insert_mode:
            self.do_insert_into(line)
        else:
            # 尝试解析自然语言查询
            sql_query = self.parse_natural_language(line)
            if sql_query:
                self.execute_sql(sql_query)
            else:
                super().default(line)
    
    def parse_natural_language(self, query):
        """解析自然语言查询并返回SQL（修复版）"""
        patterns = [
            # 1. 基础比较查询（支持数值和字符串）
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)(大于|小于|等于|不等于|大于等于|小于等于)(\d+|'[^']*')的数据",
             lambda m: self._build_comparison_query(m)),
            
            # 2. 字符串模糊匹配
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)(包含|等于|不等于)('?[^']*'?)的数据",
             lambda m: self._build_string_query(m)),
            
            # 3. 查询特定字段（支持多种分隔符）
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_，,、\s]+)的数据",
             lambda m: f"SELECT {self._normalize_fields(m.group(2))} FROM {m.group(1)}"),
            
            # 4. 查询所有数据
            (r"查询表([a-zA-Z_]+)中所有数据",
             lambda m: f"SELECT * FROM {m.group(1)}"),
            
            # 5. 改进的逻辑组合查询（AND/OR条件，支持相同字段）
            (r"查询表([a-zA-Z_]+)中(([a-zA-Z_]+)(大于|小于|等于|不等于|大于等于|小于等于)(\d+|'[^']*'))(并且|而且|且|and|或者|或|or)(([a-zA-Z_]+)(大于|小于|等于|不等于|大于等于|小于等于)(\d+|'[^']*'))的数据",
             lambda m: self._build_improved_logic_query(m, m.group(6))),
            
            # 6. 范围查询（BETWEEN）
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)(在|介于)(\d+|'[^']*')和(\d+|'[^']*')之间的数据",
             lambda m: f"SELECT * FROM {m.group(1)} WHERE {m.group(2)} BETWEEN {m.group(4)} AND {m.group(5)}"),
            
            # 7. 聚合查询
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)的(最大值|最大|最高)",
             lambda m: f"SELECT MAX({m.group(2)}) FROM {m.group(1)}"),
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)的(最小值|最小|最低)",
             lambda m: f"SELECT MIN({m.group(2)}) FROM {m.group(1)}"),
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)的(平均值|平均)",
             lambda m: f"SELECT AVG({m.group(2)}) FROM {m.group(1)}"),
            (r"查询表([a-zA-Z_]+)中([a-zA-Z_]+)的(总和|合计|总数)",
             lambda m: f"SELECT SUM({m.group(2)}) FROM {m.group(1)}"),
            (r"查询表([a-zA-Z_]+)的记录数",
             lambda m: f"SELECT COUNT(*) FROM {m.group(1)}"),
        ]
        
        try:
            for pattern, handler in patterns:
                match = re.match(pattern, query)
                if match:
                    sql = handler(match)
                    print(f"生成的SQL：{sql}")
                    return sql
            
            return None
            
        except Exception as e:
            # 捕获并处理异常，保持程序运行
            print(f"解析查询时发生错误: {str(e)}")
            return None
    
    def _build_comparison_query(self, match):
        """构建比较条件查询的SQL"""
        op_mapping = {
            "大于": ">",
            "小于": "<",
            "等于": "=",
            "不等于": "!=",
            "大于等于": ">=",
            "小于等于": "<="
        }
        operator_chi = match.group(3)
        operator = op_mapping.get(operator_chi, ">")
        value = match.group(4)  # 修正：单条件查询只有4个组
        
        # 确保字符串值被引号包围
        if not value.startswith("'") and not value.isdigit():
            value = f"'{value}'"
            
        return f"SELECT * FROM {match.group(1)} WHERE {match.group(2)} {operator} {value}"
    
    def _build_string_query(self, match):
        """构建字符串条件查询的SQL"""
        operator = "LIKE" if match.group(3) == "包含" else "="
        operator = "!=" if match.group(3) == "不等于" else operator
        value = match.group(4)
        
        # 处理LIKE查询的通配符
        if operator == "LIKE":
            if not value.startswith("'"):
                value = f"'%{value}%'"
            elif not value.startswith("'%"):
                value = f"'%{value[1:-1]}%'"
        
        # 确保字符串值被引号包围
        if not value.startswith("'") and not value.isdigit():
            value = f"'{value}'"
            
        return f"SELECT * FROM {match.group(1)} WHERE {match.group(2)} {operator} {value}"
    
    def _build_improved_logic_query(self, match, logic_op_chi):
        """构建改进的逻辑组合查询SQL（支持相同字段）"""
        # 将中文逻辑操作符转换为SQL操作符
        logic_op = "AND" if logic_op_chi.lower() in ["并且", "而且", "且", "and"] else "OR"
        
        # 提取两个条件的组件
        field1, op_chi1, val1 = match.group(3), match.group(4), match.group(5)
        field2, op_chi2, val2 = match.group(8), match.group(9), match.group(10)
        
        # 转换操作符
        op1 = self._get_operator(op_chi1)
        op2 = self._get_operator(op_chi2)
        
        # 格式化值
        val1 = self._format_value(val1)
        val2 = self._format_value(val2)
        
        # 构建完整SQL
        return f"SELECT * FROM {match.group(1)} WHERE {field1} {op1} {val1} {logic_op} {field2} {op2} {val2}"
    
    def _get_operator(self, operator_chi):
        """将中文操作符转换为SQL操作符"""
        op_mapping = {
            "大于": ">",
            "小于": "<",
            "等于": "=",
            "不等于": "!=",
            "大于等于": ">=",
            "小于等于": "<="
        }
        return op_mapping.get(operator_chi, "=")
    
    def _format_value(self, value):
        """格式化值，确保字符串被引号包围"""
        if not value.startswith("'") and not value.isdigit():
            return f"'{value}'"
        return value
    
    def _normalize_fields(self, fields_str):
        """规范化字段列表，处理各种分隔符"""
        # 替换中文逗号、顿号和空格为英文逗号
        fields_str = fields_str.replace("，", ",").replace("、", ",").replace(" ", ",")
        # 去除多余空格
        return ",".join([field.strip() for field in fields_str.split(",") if field.strip()])
    
    def _parse_condition(self, condition):
        """解析英文条件表达式"""
        # 匹配英文比较条件
        match = re.search(r"(\b[a-zA-Z_]+\b)\s*(>|>=|<|<=|=|!=)\s*(\b[a-zA-Z0-9_.']+\b)", condition)
        if match:
            field, operator, value = match.groups()
            # 处理字符串值（带单引号的直接使用，否则自动添加）
            if not value.startswith("'") and not value.isdigit():
                value = f"'{value}'"
            return f"{field} {operator} {value}"
        return condition  # 无法解析时返回原始条件
    
    def do_select_data(self, arg):
        """执行SQL查询并格式化结果输出，支持以 visualon 结尾触发可视化"""
        arg = arg.strip()
        if not arg:
            print("请提供SQL查询语句或自然语言查询")
            return

        # 检查是否需要可视化
        visualize = False
        if arg.lower().endswith('visualon'):
            visualize = True
            arg = arg[:-9].strip()  # 移除触发词

        try:
            # 执行查询
            cursor = self.db_connection.cursor()
            self.check_sql(arg)  # 检查语法
            cursor.execute(arg)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # 格式化输出
            self.print_table(results, column_names)

            # 触发可视化
            if visualize and results:
                sql_query = arg  # 使用原始查询语句（不含触发词）
                self.visualize_data(sql_query)

        except Exception as e:
            print(f"查询出错: {e}")

    def print_table(self, rows, headers):
        """以表格形式打印查询结果"""
        if not rows:
            print("结果为空")
            return
        
        # 计算各列最大宽度
        col_widths = [max(len(str(header)), max(len(str(row[i])) for row in rows)) for i, header in enumerate(headers)]
        
        # 打印分隔线
        print("-" * (sum(col_widths) + 3 * (len(headers) - 1)))
        
        # 打印表头
        header_line = " | ".join([header.ljust(col_widths[i]) for i, header in enumerate(headers)])
        print(header_line)
        
        # 打印分隔线
        print("-" * (sum(col_widths) + 3 * (len(headers) - 1)))
        
        # 打印数据行
        for row in rows:
            data_line = " | ".join([str(row[i]).ljust(col_widths[i]) for i in range(len(row))])
            print(data_line)
        
        # 打印底部分隔线
        print("-" * (sum(col_widths) + 3 * (len(headers) - 1)))

    def do_change_data(self, arg):
        """参数格式: table_name SET column1 = value1, column2 = value2 WHERE condition"""
        if self.db_connection is None:
            print("数据库连接失败，无法执行更新操作")
            return

        if not arg.strip():
            print("参数不能为空，请指定表名、SET子句和WHERE条件")
            return

        # 构建完整的 UPDATE 语句（自动添加 UPDATE 关键字）
        sql = f"UPDATE {arg};"

        try:
            self.check_sql(sql)  # 检查 SQL 语法
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            self.db_connection.commit()

            if cursor.rowcount == 0:
                print("没有找到符合条件的数据，无法进行更新操作。")
            else:
                print(f"成功更新 {cursor.rowcount} 条记录")

        except sqlite3.OperationalError as e:
            error_message = str(e)
            if "no such table" in error_message:
                table_name = arg.split()[0]  # 提取第一个词作为表名
                print(f"错误：表 {table_name} 不存在。请检查表名是否正确，或者先创建该表。")
            elif "near 'SET'" in error_message:
                print("错误：未找到 SET 关键字。请确保输入格式为 'table_name SET ... WHERE ...'")
            elif "near 'WHERE'" in error_message:
                print("错误：未找到 WHERE 关键字。请确保输入格式为 'table_name SET ... WHERE ...'")
            elif "near" in error_message:
                position = error_message.split("near ")[1].split(":")[0].strip()
                print(f"错误：在 {position} 附近存在语法错误。请检查该位置的语法。")
            else:
                print(f"执行更新操作时出错: {e}")
        except Exception as e:
            print(f"执行更新操作时出错: {e}")

    def do_delete_data(self, arg):
        """参数格式: table_name WHERE condition1 AND condition2..."""
        if self.db_connection is None:
            print("数据库连接失败，无法执行删除操作")
            return

        if not arg.strip():
            print("参数不能为空，请指定表名和条件")
            return

        # 构建完整的 DELETE 语句（仅自动添加 DELETE FROM）
        sql = f"DELETE FROM {arg};"

        try:
            self.check_sql(sql)  # 检查 SQL 语法
            cursor = self.db_connection.cursor()
            cursor.execute(sql)
            self.db_connection.commit()

            if cursor.rowcount == 0:
                print("没有找到符合条件的数据，无法进行删除操作。")
            else:
                print(f"成功删除 {cursor.rowcount} 条记录")

        except sqlite3.OperationalError as e:
            error_message = str(e)
            if "no such table" in error_message:
                table_name = arg.split()[0]  # 提取第一个词作为表名
                print(f"错误：表 {table_name} 不存在。请检查表名是否正确，或者先创建该表。")
            elif "near 'WHERE'" in error_message:
                print("错误：未找到 WHERE 关键字。请确保输入格式为 'table_name WHERE condition'")
            elif "near" in error_message:
                position = error_message.split("near ")[1].split(":")[0].strip()
                print(f"错误：在 {position} 附近存在语法错误。请检查条件语法。")
            else:
                print(f"执行删除操作时出错: {e}")
        except Exception as e:
            print(f"执行删除操作时出错: {e}")

    def do_quit(self, arg):
        """退出命令行工具"""
        print("正在退出命令行工具")
        if self.db_connection:
            self.db_connection.close()
        return True

    def preloop(self):
        """在命令循环开始前执行，用于显示表数量和表名"""
        super().preloop()
        if self.db_connection:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            print(f"数据库中共有 {len(tables)} 个关系表")
            #输出所有表名
            print("表名如下：")
            for table in tables:
                print(f"- {table[0]}")

    def import_excel_to_table(self, excel_file_path, table_name):
        """
        读取 Excel 文件并根据列名自动插入数据到指定的表中
        :param excel_file_path: Excel 文件路径
        :param table_name: 目标表名
        """
        try:
            df = pd.read_excel(excel_file_path)
            columns = df.columns.tolist()
            placeholders = ', '.join(['?'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            cursor = self.db_connection.cursor()
            for _, row in df.iterrows():
                values = tuple(row)
                cursor.execute(insert_sql, values)
            self.db_connection.commit()
            print(f"数据已成功从 {excel_file_path} 插入到表 {table_name} 中")
        except FileNotFoundError:
            print(f"文件 {excel_file_path} 不存在")
        except Exception as e:
            print(f"插入数据时出错: {e}")

    def do_import_excel(self, arg):
        """导入 Excel 文件到指定表中"""
        args = arg.split()
        if len(args) != 2:
            print("参数错误，用法: import_excel <excel_file_path> <table_name>")
            return
        excel_file_path = args[0].strip('"').strip("'")  # 去除引号
        table_name = args[1]
        self.import_excel_to_table(excel_file_path, table_name)
    
    def create_related_tables(self):
        """创建记录、用户信息、安防事件和用户反馈相关表并初始化数据"""
        if self.db_connection is None:
            print("数据库连接失败，无法创建表")
            return
        
        try:
            cursor = self.db_connection.cursor()
            
            # 1. 创建用户信息表 (user_info)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_info (
                    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    phone TEXT UNIQUE,
                    email TEXT UNIQUE
                )
            ''')
            
            # 2. 创建安防事件表 (security_event)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS security_event (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_level TEXT NOT NULL,
                    event_desc TEXT,
                    location TEXT NOT NULL,
                    occur_time DATETIME NOT NULL
                )
            ''')
            
            # 3. 创建用户反馈表 (user_feedback)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_feedback (
                    feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    feedback_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    contact_info TEXT
                )
            ''')
            
            # 4. 创建系统记录表 (system_record)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_record (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_type TEXT NOT NULL,
                    content TEXT,
                    operator_id INTEGER,
                    operate_time DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.db_connection.commit()
            print("记录、用户信息、安防事件和用户反馈相关表创建成功")
            
            # 初始化数据
            self.initialize_sample_data()
            
        except Exception as e:
            print(f"创建表时出错: {e}")
    
    def initialize_sample_data(self):
        """为各表初始化示例数据"""
        try:
            cursor = self.db_connection.cursor()
            
            # 初始化用户信息表
            users = [
                ('admin', '13800000001', 'admin@example.com'),
                ('user1', '13800000002', 'user1@example.com'),
                ('user2', '13800000003', 'user2@example.com'),
                ('user3', '13800000004', 'user3@example.com'),
                ('user4', '13800000005', 'user4@example.com')
            ]
            cursor.executemany("INSERT INTO user_info (username, phone, email) VALUES (?, ?, ?)", users)
            print(f"已为 user_info 表插入 {len(users)} 条数据")
            
            # 初始化安防事件表
            events = [
                ('入侵报警', '紧急', '发现不明人员翻越围墙', '南门', '2025-05-20 08:30:00'),
                ('火灾报警', '紧急', '烟雾传感器触发', '办公楼3层', '2025-05-21 14:15:00'),
                ('设备故障', '一般', '摄像头信号丢失', '停车场A区', '2025-05-21 16:40:00'),
                ('异常行为', '中等', '人员长时间徘徊', '金库附近', '2025-05-22 09:20:00'),
                ('系统告警', '一般', '服务器负载过高', '监控中心', '2025-05-22 11:05:00')
            ]
            cursor.executemany("INSERT INTO security_event (event_type, event_level, event_desc, location, occur_time) VALUES (?, ?, ?, ?, ?)", events)
            print(f"已为 security_event 表插入 {len(events)} 条数据")
            
            # 初始化用户反馈表
            feedbacks = [
                (1, '建议', '希望增加移动端查看功能', '13800000001'),
                (2, '投诉', '监控画面有时卡顿', '13800000002'),
                (3, '咨询', '如何申请查看历史记录', '13800000003'),
                (4, '建议', '报警推送能否增加声音提醒', '13800000004'),
                (5, '投诉', '部分区域摄像头存在死角', '13800000005')
            ]
            cursor.executemany("INSERT INTO user_feedback (user_id, feedback_type, content, contact_info) VALUES (?, ?, ?, ?)", feedbacks)
            print(f"已为 user_feedback 表插入 {len(feedbacks)} 条数据")
            
            # 初始化系统记录表
            records = [
                ('登录', '管理员登录系统', 1, '2025-05-22 08:00:00'),
                ('操作', '修改了报警阈值设置', 1, '2025-05-22 08:15:00'),
                ('添加', '添加了新摄像头设备', 1, '2025-05-22 09:30:00'),
                ('处理', '处理了南门入侵报警事件', 2, '2025-05-22 10:45:00'),
                ('导出', '导出了本周报警记录', 1, '2025-05-22 11:30:00')
            ]
            cursor.executemany("INSERT INTO system_record (record_type, content, operator_id, operate_time) VALUES (?, ?, ?, ?)", records)
            print(f"已为 system_record 表插入 {len(records)} 条数据")
            
            self.db_connection.commit()
            print("所有表数据初始化完成")
            
        except Exception as e:
            self.db_connection.rollback()
            print(f"初始化数据时出错: {e}")
    
    def do_create_related_tables(self, arg):
        """创建记录、用户信息、安防事件和用户反馈相关表并初始化数据"""
        self.create_related_tables()

    def do_init_db(self, arg):
        """
        手动初始化命令行工具，删除所有表，并重新进入数据库
        """
        if self.db_connection is None:
            print("数据库连接失败，无法进行初始化操作")
            return
        try:
            # 获取所有表名
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':
                    try:
                        # 删除表
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception as e:
                        print(f"表 {table_name} 无法删除，错误信息: {e}")

            self.db_connection.commit()
            print("所有表已删除，正在重新连接数据库...")
            self.db_connection.close()
            self.db_connection = sqlite3.connect(self.db_file)
            print("数据库已重新连接，初始化完成。")
        except Exception as e:
            print(f"手动初始化时出错: {e}")

if __name__ == '__main__':
    # 默认不重置系统
    tool = MyCommandLineTool()
    tool.cmdloop()