#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import logging
import os
import math
import numpy as np
import cv2
from functools import cmp_to_key


from api.utils.file_utils import get_project_base_directory
from .operators import *  # noqa: F403
from .operators import preprocess
from . import operators
from .ocr import load_model

class Recognizer:
    def __init__(self, label_list, task_name, model_dir=None):
        """
        If you have trouble downloading HuggingFace models, -_^ this might help!!

        For Linux:
        export HF_ENDPOINT=https://hf-mirror.com

        For Windows:
        Good luck
        ^_-

        """
        if not model_dir:
            model_dir = os.path.join(
                        get_project_base_directory(),
                        "rag/res/deepdoc")
        self.ort_sess, self.run_options = load_model(model_dir, task_name)
        self.input_names = [node.name for node in self.ort_sess.get_inputs()]
        self.output_names = [node.name for node in self.ort_sess.get_outputs()]
        self.input_shape = self.ort_sess.get_inputs()[0].shape[2:4]
        self.label_list = label_list

    @staticmethod
    def sort_Y_firstly(arr, threshold):
        """
        arr: 一个包含字典的列表，每个字典代表一个检测框，必须包含 "top"（上边缘 y 坐标）和 "x0"（左边缘 x 坐标）。
        threshold: 判定的阈值。如果两个框的垂直距离小于这个值，就被视为同一行。
        """
        def cmp(c1, c2):
            # 1. 首先计算两个元素在垂直方向（top）上的差距
            diff = c1["top"] - c2["top"]
            
            # 2. 关键判断：如果垂直差距的绝对值小于阈值（threshold）
            # 这意味着两个元素在垂直方向上非常接近，可以认为它们在同一行
            if abs(diff) < threshold:
                # 在同一行的情况下，改为比较水平方向（x0），即从左到右排序
                diff = c1["x0"] - c2["x0"]
            
            # 3. 返回差值
            # 如果 diff < 0，说明 c1 排在 c2 前面
            # 如果 diff > 0，说明 c1 排在 c2 后面
            return diff
        # 使用 cmp_to_key 将比较函数转换为 sorted 函数所需的 key
        arr = sorted(arr, key=cmp_to_key(cmp))
        return arr

    @staticmethod
    def sort_X_firstly(arr, threshold):
        def cmp(c1, c2):
            diff = c1["x0"] - c2["x0"]
            if abs(diff) < threshold:
                diff = c1["top"] - c2["top"]
            return diff
        arr = sorted(arr, key=cmp_to_key(cmp))
        return arr

    @staticmethod
    def sort_C_firstly(arr, thr=0):
        # sort using y1 first and then x1
        # sorted(arr, key=lambda r: (r["x0"], r["top"]))
        arr = Recognizer.sort_X_firstly(arr, thr)
        for i in range(len(arr) - 1):
            for j in range(i, -1, -1):
                # restore the order using th
                if "C" not in arr[j] or "C" not in arr[j + 1]:
                    continue
                if arr[j + 1]["C"] < arr[j]["C"] \
                        or (
                        arr[j + 1]["C"] == arr[j]["C"]
                        and arr[j + 1]["top"] < arr[j]["top"]
                ):
                    tmp = arr[j]
                    arr[j] = arr[j + 1]
                    arr[j + 1] = tmp
        return arr

    @staticmethod
    def sort_R_firstly(arr, thr=0):
        # sort using y1 first and then x1
        # sorted(arr, key=lambda r: (r["top"], r["x0"]))
        arr = Recognizer.sort_Y_firstly(arr, thr)
        for i in range(len(arr) - 1):
            for j in range(i, -1, -1):
                if "R" not in arr[j] or "R" not in arr[j + 1]:
                    continue
                if arr[j + 1]["R"] < arr[j]["R"] \
                        or (
                        arr[j + 1]["R"] == arr[j]["R"]
                        and arr[j + 1]["x0"] < arr[j]["x0"]
                ):
                    tmp = arr[j]
                    arr[j] = arr[j + 1]
                    arr[j + 1] = tmp
        return arr

    @staticmethod
    def overlapped_area(a, b, ratio=True):
        tp, btm, x0, x1 = a["top"], a["bottom"], a["x0"], a["x1"]
        if b["x0"] > x1 or b["x1"] < x0:
            return 0
        if b["bottom"] < tp or b["top"] > btm:
            return 0
        x0_ = max(b["x0"], x0)
        x1_ = min(b["x1"], x1)
        assert x0_ <= x1_, "Bbox mismatch! T:{},B:{},X0:{},X1:{} ==> {}".format(
            tp, btm, x0, x1, b)
        tp_ = max(b["top"], tp)
        btm_ = min(b["bottom"], btm)
        assert tp_ <= btm_, "Bbox mismatch! T:{},B:{},X0:{},X1:{} => {}".format(
            tp, btm, x0, x1, b)
        ov = (btm_ - tp_) * (x1_ - x0_) if x1 - \
                                           x0 != 0 and btm - tp != 0 else 0
        if ov > 0 and ratio:
            ov /= (x1 - x0) * (btm - tp)
        return ov

    @staticmethod
    def layouts_cleanup(boxes, layouts, far=2, thr=0.7):
        def not_overlapped(a, b):
            # 满足任意条件就可以断定没有重叠
            return any([a["x1"] < b["x0"],  # a 的右侧在 b 的左侧，即 a 位于 b 左侧
                        a["x0"] > b["x1"],  # a 的左侧在 b 的右侧，即 a 位于 b 右侧
                        a["bottom"] < b["top"],  # a 的下边缘在 b 的上边缘上方，即 a 位于 b 上方
                        a["top"] > b["bottom"]])  # a 的上边缘在 b 的下边缘下方，即 a 位于 b 下方

        i = 0
        while i + 1 < len(layouts):
            j = i + 1
            # 限制比较范围，只检查 i 后面最多 far 个布局，避免全列表比较，提高性能
            # 如果两个布局的 type 不同，或者不重叠，继续
            # 代码的逻辑是局部优先而非全局优先。在找到一个冲突后，只处理该冲突，但不会重新检查 i 与 far 范围内未检查元素的其他冲突
            while j < min(i + far, len(layouts)) \
                    and (layouts[i].get("type", "") != layouts[j].get("type", "")
                         or not_overlapped(layouts[i], layouts[j])):
                j += 1
            # 如果在 i+1 到 i+far 范围内，没有找到任何与 layouts[i] 类型相同且重叠的布局，外循环 i + 1
            if j >= min(i + far, len(layouts)):
                i += 1
                continue
            # 如果 layouts[i] 和 layouts[j] 的重叠面积（互相计算）都小于阈值 thr，外循环 i + 1
            if Recognizer.overlapped_area(layouts[i], layouts[j]) < thr \
                    and Recognizer.overlapped_area(layouts[j], layouts[i]) < thr:
                i += 1
                continue

            # 重叠的情况下，分数高的布局被保留
            if layouts[i].get("score") and layouts[j].get("score"):
                if layouts[i]["score"] > layouts[j]["score"]:
                    layouts.pop(j)
                else:
                    layouts.pop(i)
                continue

            # 如果两个布局没有分数或分数相等，计算它们与 boxes 列表的重叠面积
            # 初始化 area_i 和 area_i_1 为 0，分别存储 layouts[i] 和 layouts[j] 与 boxes 的重叠面积
            area_i, area_i_1 = 0, 0
            # 遍历 boxes 列表，检查每个参考框 b 是否与当前布局重叠
            for b in boxes:
                # 如果重叠，则累加该框与布局的重叠面积
                if not not_overlapped(b, layouts[i]):
                    area_i += Recognizer.overlapped_area(b, layouts[i], False)
                if not not_overlapped(b, layouts[j]):
                    area_i_1 += Recognizer.overlapped_area(b, layouts[j], False)

            # 保留重叠面积大的
            if area_i > area_i_1:
                layouts.pop(j)
            else:
                layouts.pop(i)

        return layouts

    def create_inputs(self, imgs, im_info):
        """generate input for different model type
        Args:
            imgs (list(numpy)): list of images (np.ndarray)
            im_info (list(dict)): list of image info
        Returns:
            inputs (dict): input of model
        """
        inputs = {}

        im_shape = []
        scale_factor = []
        if len(imgs) == 1:
            inputs['image'] = np.array((imgs[0],)).astype('float32')
            inputs['im_shape'] = np.array(
                (im_info[0]['im_shape'],)).astype('float32')
            inputs['scale_factor'] = np.array(
                (im_info[0]['scale_factor'],)).astype('float32')
            return inputs
        
        im_shape = np.array([info['im_shape'] for info in im_info], dtype='float32')
        scale_factor = np.array([info['scale_factor'] for info in im_info], dtype='float32')

        inputs['im_shape'] = np.concatenate(im_shape, axis=0)
        inputs['scale_factor'] = np.concatenate(scale_factor, axis=0)

        imgs_shape = [[e.shape[1], e.shape[2]] for e in imgs]
        max_shape_h = max([e[0] for e in imgs_shape])
        max_shape_w = max([e[1] for e in imgs_shape])
        padding_imgs = []
        for img in imgs:
            im_c, im_h, im_w = img.shape[:]
            padding_im = np.zeros(
                (im_c, max_shape_h, max_shape_w), dtype=np.float32)
            padding_im[:, :im_h, :im_w] = img
            padding_imgs.append(padding_im)
        inputs['image'] = np.stack(padding_imgs, axis=0)
        return inputs

    @staticmethod
    def find_overlapped(box, boxes_sorted_by_y, naive=False):
        if not boxes_sorted_by_y:
            return
        bxs = boxes_sorted_by_y
        s, e, ii = 0, len(bxs), 0
        while s < e and not naive:
            ii = (e + s) // 2
            pv = bxs[ii]
            if box["bottom"] < pv["top"]:
                e = ii
                continue
            if box["top"] > pv["bottom"]:
                s = ii + 1
                continue
            break
        while s < ii:
            if box["top"] > bxs[s]["bottom"]:
                s += 1
            break
        while e - 1 > ii:
            if box["bottom"] < bxs[e - 1]["top"]:
                e -= 1
            break

        max_overlapped_i, max_overlapped = None, 0
        for i in range(s, e):
            ov = Recognizer.overlapped_area(bxs[i], box)
            if ov <= max_overlapped:
                continue
            max_overlapped_i = i
            max_overlapped = ov

        return max_overlapped_i

    @staticmethod
    def find_horizontally_tightest_fit(box, boxes):
        if not boxes:
            return
        min_dis, min_i = 1000000, None
        for i,b in enumerate(boxes):
            if box.get("layoutno", "0") != b.get("layoutno", "0"):
                continue
            dis = min(abs(box["x0"] - b["x0"]), abs(box["x1"] - b["x1"]), abs(box["x0"]+box["x1"] - b["x1"] - b["x0"])/2)
            if dis < min_dis:
                min_i = i
                min_dis = dis
        return min_i

    @staticmethod
    def find_overlapped_with_threshold(box, boxes, thr=0.3):
        if not boxes:
            return
        max_overlapped_i, max_overlapped, _max_overlapped = None, thr, 0
        s, e = 0, len(boxes)
        for i in range(s, e):
            # ov: box 与 boxes[i] 的重叠面积占 box 的比例
            ov = Recognizer.overlapped_area(box, boxes[i])
            # _ov: 表示 boxes[i] 与 box 的重叠面积占 boxes[i] 的比例
            _ov = Recognizer.overlapped_area(boxes[i], box)
            # 检查当前矩形的正向和反向重叠面积是否都大于当前记录的最大值
            if (ov, _ov) < (max_overlapped, _max_overlapped):
                continue
            max_overlapped_i = i
            max_overlapped = ov
            _max_overlapped = _ov

        return max_overlapped_i

    # 预处理，将图像列表转换为模型输入格式
    def preprocess(self, image_list):
        inputs = []
        if "scale_factor" in self.input_names:
            preprocess_ops = []
            for op_info in [
                {'interp': 2, 'keep_ratio': False, 'target_size': [800, 608], 'type': 'LinearResize'},
                {'is_scale': True, 'mean': [0.485, 0.456, 0.406], 'std': [0.229, 0.224, 0.225], 'type': 'StandardizeImage'},
                {'type': 'Permute'},
                {'stride': 32, 'type': 'PadStride'}
            ]:
                new_op_info = op_info.copy()
                op_type = new_op_info.pop('type')
                preprocess_ops.append(getattr(operators, op_type)(**new_op_info))

            for im_path in image_list:
                im, im_info = preprocess(im_path, preprocess_ops)
                inputs.append({"image": np.array((im,)).astype('float32'),
                               "scale_factor": np.array((im_info["scale_factor"],)).astype('float32')})
        else:
            hh, ww = self.input_shape
            for img in image_list:
                h, w = img.shape[:2]
                img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                img = cv2.resize(np.array(img).astype('float32'), (ww, hh))
                # Scale input pixel values to 0 to 1
                img /= 255.0
                img = img.transpose(2, 0, 1)
                img = img[np.newaxis, :, :, :].astype(np.float32)
                inputs.append({self.input_names[0]: img, "scale_factor": [w/ww, h/hh]})
        return inputs

    def postprocess(self, boxes, inputs, thr):
        if "scale_factor" in self.input_names:
            bb = []
            for b in boxes:
                clsid, bbox, score = int(b[0]), b[2:], b[1]
                if score < thr:
                    continue
                if clsid >= len(self.label_list):
                    continue
                bb.append({
                    "type": self.label_list[clsid].lower(),
                    "bbox": [float(t) for t in bbox.tolist()],
                    "score": float(score)
                })
            return bb

        def xywh2xyxy(x):
            # [x, y, w, h] to [x1, y1, x2, y2]
            y = np.copy(x)
            y[:, 0] = x[:, 0] - x[:, 2] / 2
            y[:, 1] = x[:, 1] - x[:, 3] / 2
            y[:, 2] = x[:, 0] + x[:, 2] / 2
            y[:, 3] = x[:, 1] + x[:, 3] / 2
            return y

        def compute_iou(box, boxes):
            # Compute xmin, ymin, xmax, ymax for both boxes
            xmin = np.maximum(box[0], boxes[:, 0])
            ymin = np.maximum(box[1], boxes[:, 1])
            xmax = np.minimum(box[2], boxes[:, 2])
            ymax = np.minimum(box[3], boxes[:, 3])

            # Compute intersection area
            intersection_area = np.maximum(0, xmax - xmin) * np.maximum(0, ymax - ymin)

            # Compute union area
            box_area = (box[2] - box[0]) * (box[3] - box[1])
            boxes_area = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
            union_area = box_area + boxes_area - intersection_area

            # Compute IoU
            iou = intersection_area / union_area

            return iou

        def iou_filter(boxes, scores, iou_threshold):
            sorted_indices = np.argsort(scores)[::-1]

            keep_boxes = []
            while sorted_indices.size > 0:
                # Pick the last box
                box_id = sorted_indices[0]
                keep_boxes.append(box_id)

                # Compute IoU of the picked box with the rest
                ious = compute_iou(boxes[box_id, :], boxes[sorted_indices[1:], :])

                # Remove boxes with IoU over the threshold
                keep_indices = np.where(ious < iou_threshold)[0]

                # print(keep_indices.shape, sorted_indices.shape)
                sorted_indices = sorted_indices[keep_indices + 1]

            return keep_boxes

        boxes = np.squeeze(boxes).T
        # Filter out object confidence scores below threshold
        scores = np.max(boxes[:, 4:], axis=1)
        boxes = boxes[scores > thr, :]
        scores = scores[scores > thr]
        if len(boxes) == 0:
            return []

        # Get the class with the highest confidence
        class_ids = np.argmax(boxes[:, 4:], axis=1)
        boxes = boxes[:, :4]
        input_shape = np.array([inputs["scale_factor"][0], inputs["scale_factor"][1], inputs["scale_factor"][0], inputs["scale_factor"][1]])
        boxes = np.multiply(boxes, input_shape, dtype=np.float32)
        boxes = xywh2xyxy(boxes)

        unique_class_ids = np.unique(class_ids)
        indices = []
        for class_id in unique_class_ids:
            class_indices = np.where(class_ids == class_id)[0]
            class_boxes = boxes[class_indices, :]
            class_scores = scores[class_indices]
            class_keep_boxes = iou_filter(class_boxes, class_scores, 0.2)
            indices.extend(class_indices[class_keep_boxes])

        return [{
            "type": self.label_list[class_ids[i]].lower(),
            "bbox": [float(t) for t in boxes[i].tolist()],
            "score": float(scores[i])
        } for i in indices]

    def __call__(self, image_list, thr=0.7, batch_size=16):
        res = []
        images = []
        for i in range(len(image_list)):
            if not isinstance(image_list[i], np.ndarray):
                images.append(np.array(image_list[i]))
            else:
                images.append(image_list[i])

        batch_loop_cnt = math.ceil(float(len(images)) / batch_size)
        for i in range(batch_loop_cnt):
            start_index = i * batch_size
            end_index = min((i + 1) * batch_size, len(images))
            batch_image_list = images[start_index:end_index]
            # NOTE: Recognizer 1. 先预处理，将图像列表转换为模型输入格式
            inputs = self.preprocess(batch_image_list)
            logging.debug("preprocess")
            for ins in inputs:
                # NOTE: Recognizer 2. 然后调用 ort_sess 执行 onnx 推理
                # NOTE: Recognizer 3. 最后 postprocess，提取模型返回的布局信息，包括区域类型、坐标和置信度
                bb = self.postprocess(self.ort_sess.run(None, {k:v for k,v in ins.items() if k in self.input_names}, self.run_options)[0], ins, thr)
                res.append(bb)

        #seeit.save_results(image_list, res, self.label_list, threshold=thr)

        return res



