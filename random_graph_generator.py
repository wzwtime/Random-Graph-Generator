# coding=utf-8
"""产生随机图2018-03-26"""
import operator
import random
import math
import os
# import networkx as nx
# D = nx.DiGraph

SET_v = [20, 40, 60, 80, 100]
SET_ccr = [0.1, 0.5, 1.0, 5.0, 10.0]
SET_alpha = [1.0, 2.0]
SET_out_degree = [2, 3, 4, 5, ]
SET_beta = [0.1, 0.25, 0.5, 0.75, 1.0]
computation_costs = []
dag = {}


def random_avg_w_dag(n_min, n_max):
    """随机产生平均计算花销"""
    avg_w_dag_ = random.randint(n_min, n_max)
    return avg_w_dag_


# 随机产生平均计算花销
avg_w_dag = random_avg_w_dag(5, 20)


def get_wij(v, p, beta):
    """产生任务在不同处理器上的计算开销"""
    filename = 'computation_costs.txt'
    os.remove(filename)  # 删除文件
    for i in range(v):
        # 每个任务的平均计算开销avg_w
        avg_w = random.randint(1, 2 * avg_w_dag)
        # print(avg_w)
        temp_list = []
        for j in range(p):
            # 每一个任务i在处理器j上的计算开销
            wij = random.randint(math.ceil(avg_w * (1 - beta / 2)), math.ceil(avg_w * (1 + beta / 2)))
            temp_list.append(wij)
        computation_costs.append(temp_list)
        # 写入文件中
        with open(filename, 'a') as file_object:
            info = str(temp_list) + "\n"
            file_object.write(info)


def get_height_width(v, alpha):
    """获取图的高度和宽度"""
    mean_height = math.ceil(math.sqrt(v) / alpha)  # 向上取整，计算平均值
    mean_width = math.ceil(alpha * math.sqrt(v))  # 向上取整，计算平均值
    height = random.randint(1, 2 * mean_height - 1)  # 深度选择以mean_height为均值的均匀随机分布
    width = random.randint(1, 2 * mean_width - 1)  # 宽度选择以mean_width为均值的均匀随机分布

    # print("mean_height =", mean_height, "mean_width =", mean_width)
    return height, width


def random_graph_generator(v, ccr, alpha, out_degree, beta, p):
    """requires five parameters to build weighted DAGs
    v: number of tasks in the graph
    ccr: average communication cost to average computation cost
    alpha: shape parameter of the graph
    out_degree: out degree of a node
    beta: range percentage of computation costs on processors
    p: number of processors"""
    # print(v, ccr, alpha, out_degree, beta)
    height = get_height_width(v, alpha)[0]
    width = get_height_width(v, alpha)[1]

    # 产生DAG图

    # 建图
    # 判断是否可以构成DAG图
    while True:
        # 判断是否可以构成DAG图
        if (height - 3) * width >= v - 2 - width:
            print("v =", v, "height = ", height, "width =", width, "CCR =", ccr, "Alpha =", alpha,
                  "out_degree =", out_degree, "beta =", beta, "Number of Processors =", p)
            print("Yes")
            break
        else:
            height = get_height_width(v, alpha)[0]
            width = get_height_width(v, alpha)[1]

    # 1) 首先是将图每层的节点数确定
    num_second_layer = random.randint(2, out_degree)
    while True:
        task_num_layer = []
        for j in range(height - 4):
            num_task = random.randint(2, width)
            task_num_layer.append(num_task)
        if sum(task_num_layer) == v - 2 - width - num_second_layer:
            break
    task_num_layer.insert(0, 1)
    task_num_layer.insert(1, num_second_layer)
    task_num_layer.insert(int(height/2), width)
    task_num_layer.append(1)
    print("original task_num_layer:", task_num_layer)

    # 根据出度排序每层的结点数
    for j in range(height - 1):
        for i in range(height - 1):
            if task_num_layer[i] * out_degree < task_num_layer[i + 1]:
                temp = task_num_layer[i]
                task_num_layer[i] = task_num_layer[i + 1]
                task_num_layer[i + 1] = temp
    print("ordered task_num_layer:", task_num_layer)

    #  将每层的结点数转化为顺序任务编号dag_id
    dag_id = []
    num = 0
    for i in range(height):
        dag_id_temp = []
        for j in range(int(task_num_layer[i])):
            num += 1
            dag_id_temp.append(num)
        dag_id.append(dag_id_temp)
    print("dag_id = ", dag_id)

    # 2)然后再根据出度确定顶点的连接关系，分配通信开销
    # 产生任务在不同处理器上的计算开销
    get_wij(v, p, beta)
    # 平均通信开销
    avg_communication_costs = math.ceil(ccr * avg_w_dag)  # 向上取整

    # 通信开销选择以平均通信开销为均值的均匀随机分布
    # communication_costs = random.randint(1, 2 * avg_communication_costs - 1)

    # 若第一层是一个结点
    if task_num_layer[0] == 1:
        print("Truly DAG")
        # 遍历DAG的每层
        for h in range(height):
            temp_dag = {}
            if h == 0:  # 第一层
                for i in range(len(dag_id[1])):
                    index = dag_id[1][i]
                    communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                    temp_dag[index] = communication_costs
                    # print("temp_dag", temp_dag)
                dag[h + 1] = temp_dag
            elif h == height - 2:  # 倒数第二层
                for i in range(len(dag_id[height - 2])):
                    temp_dag = {}  # 注意!!!!!!!!!!! 防止产生相同的通信开销
                    index = dag_id[height - 2][i]
                    dag_index = dag_id[height - 1][0]
                    communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                    temp_dag[dag_index] = communication_costs
                    # print("temp_dag", temp_dag)
                    dag[index] = temp_dag
                    # print(dag)
            elif h != height - 1:   # 再除去最后一层的其他层
                # 少对多-将子结点随机分为父结点总数个部分
                child_num = []
                for i in range(1, len(task_num_layer) - 2):
                    # print(task_num_layer[i])
                    p_num = task_num_layer[i]
                    c_num = task_num_layer[i + 1]
                    # print(p_num, c_num)
                    if p_num != 1 and c_num != 1 and p_num <= c_num:
                        # print("okkk")
                        p_index = i
                        # 将父结点随机分为子节点总数个部分
                        while True:
                            temp_child_num = []
                            for k in range(p_num):
                                rand_child_num = random.randint(1, out_degree)
                                temp_child_num.append(rand_child_num)
                            if sum(temp_child_num) == c_num:
                                break
                        # print("temp_child_num =", temp_child_num)
                        child_num.append(temp_child_num)
                        # 遍历所在索引的每一个父结点
                        # print(p_num)
                        sum_num = 0
                        sum_list = 0
                        temp_dag = {}
                        for j in range(p_num):
                            temp_dag = {}
                            p_id = dag_id[p_index][j]
                            # print("p_id = ", p_id)
                            # 子结点编号的确定
                            # sum_num = p_id + p_num - j - 1    # 最后一个父结点编号
                            # print("last_parent_id = ", sum_num)

                            # print("temp_child_num[j] = ", temp_child_num[j])
                            if j > 0:
                                sum_list += temp_child_num[j - 1]
                                # print("sum_list = ", sum_list)
                            # 查看子节点编号
                            for k in range(temp_child_num[j]):
                                if j == 0:
                                    sum_num = p_id + p_num - j - 1 + k + 1
                                elif j > 0:
                                    sum_num = p_id + p_num - j - 1 + k + 1 + sum_list
                                # print("sum_num = ", sum_num)

                                # 分配通信开销
                                communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                                temp_dag[sum_num] = communication_costs
                            dag[p_id] = temp_dag
                # print("child_num = ", child_num)

                # 多对少-将父结点随机分为子结点总数个部分
                parent_num = []
                for i in range(2, len(task_num_layer) - 1):     # len(task_num_layer) - 1！！！！！！
                    # print(task_num_layer[i])
                    p_num = task_num_layer[i - 1]
                    c_num = task_num_layer[i]
                    # print(p_num, c_num)
                    if p_num != 1 and c_num != 1 and p_num > c_num:
                        # print("p_num =", p_num, "c_num =", c_num)
                        c_index = i
                        # 将父结点随机分为子结点总数个部分
                        while True:
                            temp_parent_num = []
                            for k in range(c_num):
                                rand_parent_num = random.randint(1, out_degree)
                                temp_parent_num.append(rand_parent_num)
                            if sum(temp_parent_num) == p_num:
                                break
                        # print("temp_parent_num =", temp_parent_num)
                        # parent_num.append(temp_parent_num)

                        # # 遍历所在索引的每一个子结点
                        # print(p_num)
                        length_parent = 0
                        for j in range(c_num):
                            temp_dag = {}
                            c_id = dag_id[c_index][j]
                            # print("c_id = ", c_id)
                            # print("temp_parent_num[j] = ", temp_parent_num[j])

                            first_parent_id = c_id - p_num

                            # 查看父节点编号
                            for k in range(temp_parent_num[j]):  # 遍历父结点的随机数量数组
                                length_parent += 1
                                # print("length_parent=", length_parent)
                                p_id = first_parent_id + length_parent - j - 1
                                # print("p_id =", p_id)

                                # 分配通信开销
                                temp_dag = {}   # 注意!!!!!!!!!!! 防止产生相同的通信开销
                                communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                                temp_dag[c_id] = communication_costs
                                dag[p_id] = temp_dag
                # print("parent_num = ", parent_num)

                # 分配父结点或子结点只有一个任务时的通信开销

                # for layer_id in range(2, height - 1):  # 层编号 2 到 height - 2
                #     # print("layer_id", layer_id)
                #     parent_length = len(dag_id[layer_id - 1])
                #     child_length = len(dag_id[layer_id])
                #
                #     max_out_degree = min(5, child_length)  # 每个结点的最大出度
                #     rand_degree = random.randint(1, max_out_degree)
                #     # print("max_out_degree", max_out_degree)
                #     # print("rand_degree", rand_degree)
                #
                #     # 分配父结点或子结点只有一个任务时的通信开销
                #     for i in range(parent_length):
                #         parent_task_id = dag_id[layer_id - 1][i]    # 父亲结点编号
                #         # print("parent_task_id =", parent_task_id)  # 显示本层的任务编号
                #         temp_dag = {}
                #         for j in range(child_length):
                #             child_task_id = dag_id[layer_id][j]     # 孩子结点编号
                #             # print("child_task_id", child_task_id)  # 显示下一层的任务编号
                #             # 若父结点只有一个
                #             if parent_length == 1:
                #                 communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                #                 temp_dag[child_task_id] = communication_costs
                #
                #             # 若子结点只有一个
                #             elif child_length == 1:
                #                 communication_costs = random.randint(1, 2 * avg_communication_costs - 1)
                #                 temp_dag[child_task_id] = communication_costs
                #                 dag[parent_task_id] = temp_dag
                #
                #         if parent_length == 1:
                #             dag[parent_task_id] = temp_dag
            else:
                # 最后一个结点
                dag[v] = {}
    else:
        print("DAG Error!")


def random_index(set_):
    """获取集合的随机索引i,以确定选择集合中哪个参数"""
    length = len(set_)
    index_ = random.randint(1, length) - 1
    return index_


def select_parameter():
    """选择5个参数"""
    v = SET_v[random_index(SET_v)]
    ccr = SET_ccr[random_index(SET_ccr)]
    alpha = SET_alpha[random_index(SET_alpha)]
    out_degree = SET_out_degree[random_index(SET_out_degree)]
    beta = SET_beta[random_index(SET_beta)]
    p = random.randint(2, 5)    # 处理器数2-5个
    # 写入文件中
    # filename = 'graph_parameter.txt'
    # with open(filename, 'w') as file_object:
    #     info = str(v) + "  " + str(ccr) + "  " + str(alpha) + "  " + str(out_degree) + "  " + str(beta) + "\n"
    #     file_object.write(info)
    random_graph_generator(20, ccr, alpha, 5, beta, 3)


select_parameter()
dag1 = sorted(dag.items(), key=operator.itemgetter(0))  # 按任务编号升序排序
print("dag =", dag1)
print("computation_costs =", computation_costs)

# dag1 = [(1, {2: 94, 3: 1}), (2, {4: 51}), (3, {5: 51}), (4, {6: 51}), (5, {7: 92}), (6, {8: 8, 9: 42, 10: 42}), (7, {11: 29}), (8, {12: 66}), (9, {13: 96}), (10, {14: 5}), (11, {15: 127}), (12, {16: 72}), (13, {16: 12}), (14, {17: 71}), (15, {17: 82}), (16, {18: 77}), (17, {19: 6}), (18, {20: 59}), (19, {20: 109}), (20, {})]


# 将DAG图存放在文件中
filename_ = 'dag.txt'
if os.path.exists(filename_):
    os.remove(filename_)  # 删除文件
for i in range(len(dag1)):
    task_id = dag1[i][0]
    for key, value in dag1[i][1].items():
        succ_id = key
        succ_weight = value
        filename_ = 'dag.txt'   # 再新建文件
        with open(filename_, 'a') as file_object:
            info = str(task_id) + " " + str(succ_id) + " " + str(succ_weight) + "\n"
            file_object.write(info)


# 读DAG文件,构造dag
new_dag = {}
filename = 'dag.txt'
if os.path.exists(filename):    # dag.txt exists
    with open(filename, 'r') as file_object:
        lines = file_object.readlines()
        task_id_ = 0
        for i in range(len(dag1)):
            succ_dict = {}  # 后继字典
            for line in lines:
                line_list = line.split()  # 默认以空格为分隔符对字符串进行切片
                task_id = int(line_list[0])
                succ_id = int(line_list[1])
                succ_weight = int(line_list[2])

                if task_id == int(dag1[i][0]):
                    task_id_ = task_id
                    succ_dict[succ_id] = succ_weight
            if task_id_ == int(dag1[i][0]):
                new_dag[task_id_] = succ_dict
    last_task_id = dag1[-1][0]
    new_dag[last_task_id] = {}

# print(computation_costs)
# print(new_dag)
# print(len(new_dag))
