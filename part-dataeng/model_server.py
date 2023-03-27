from google.cloud import storage
import numpy as np
from typing import List
import nest_asyncio
import uvicorn
from pydantic import BaseModel
import json
from PIL import Image
from io import BytesIO
from fastapi import FastAPI, Response
import skimage
import cv2
import ssl
from tensorflow import keras
from fastapi.responses import FileResponse


rgb2value = {
    (0, 0, 0): 0.0,
    (0, 255, 128): 5.0,
    (0, 255, 0): 10.0,
    (0, 175, 0): 15.0,
    (0, 150, 50): 20.0,
    (255, 255, 0): 25.0,
    (255, 200, 0): 30.0,
    (255, 170, 0): 35.0,
    (255, 85, 0): 40.0,
    (255, 0, 0): 45.0,
    (255, 0, 100): 50.0,
    (255, 0, 255): 55.0,
    (255, 128, 255): 60.0,
    (255, 200, 255): 65.0,
    (255, 225, 255): 70.0,
    (255, 255, 255): 75.0,
}
value2rgb = {int(v): k for k, v in rgb2value.items()}


model = keras.models.load_model('./rainnet_model')
ssl._create_default_https_context = ssl._create_unverified_context
app = FastAPI(title='predictor')


def convert_pic_to_array(pic):
    im = Image.open(BytesIO(pic.content))
    return np.array(im)


def rgb2intensity(pixel):
    pixel = tuple(pixel)
    if pixel == (0, 0, 0):
        return 0.0
    elif pixel == (0, 255, 128):
        return 5.0
    elif pixel == (0, 255, 0):
        return 10.0
    elif pixel == (0, 175, 0):
        return 15.0
    elif pixel == (0, 150, 50):
        return 20.0
    elif pixel == (255, 255, 0):
        return 25.0
    elif pixel == (255, 200, 0):
        return 30.0
    elif pixel == (255, 170, 0):
        return 35.0
    elif pixel == (255, 85, 0):
        return 40.0
    elif pixel == (255, 0, 0):
        return 45.0
    elif pixel == (255, 0, 100):
        return 50.0
    elif pixel == (255, 0, 255):
        return 55.0
    elif pixel == (255, 128, 255):
        return 60.0
    elif pixel == (255, 200, 255):
        return 65.0
    elif pixel == (255, 225, 255):
        return 70.0
    else:
        return 75.0


def changeToClosetValue(v):
    dif = 1000
    new_value = v
    for k in rgb2value:
        tmp = list(k)
        d = abs(tmp[0]-v[0]) + abs(tmp[1]-v[1]) + abs(tmp[2]-v[2])
        if d < dif:
            new_value = tmp
            dif = d
    return new_value


def pre_processing(pil_img):
    width, height = pil_img.size
    for i in range(width):
        for j in range(height):
            pixel_value = pil_img.getpixel((i, j))
            if pixel_value == (165, 151, 95):
                pil_img.putpixel((i, j), (0, 0, 0))
            else:
                pil_img.putpixel((i, j), tuple(
                    changeToClosetValue(pixel_value)))
    input_map = np.array(pil_img)
    intensity_img = np.zeros(
        (input_map.shape[0], input_map.shape[1])).astype('float32')
    # rgb2intersity
    for i in range(input_map.shape[0]):
        for j in range(input_map.shape[1]):
            intensity_img[i, j] = rgb2intensity(input_map[i, j])
    intensity_img /= 75
    intensity_img = cv2.resize(
        intensity_img, (928, 928), interpolation=cv2.INTER_NEAREST)
    intensity_img = np.log(intensity_img+0.01)
    return intensity_img


def intensity2rgb(intensity):
    return value2rgb[intensity]


def append_picture(req):
    img_list = list()
    url = 'https://storage.googleapis.com/datasci-kalampree/'
    for img_name in req:
        print(f'Converting {img_name}')
        img = skimage.io.imread(url+img_name)
        img = pre_processing(Image.fromarray(img).convert('RGB'))
        print(f'{img_name} is converted')
        img_list.append(img)
    img_list = np.asarray(img_list)
    img_list = img_list[np.newaxis, :, :]
    img_list = np.moveaxis(img_list, 1, -1)
    print(img_list.shape)
    return img_list


def NormalizeData(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))


def post_processing(pred_img):
    tmp_img = np.squeeze(np.array(pred_img))
    tmp_img = np.exp(tmp_img) - 0.01
    tmp_img = cv2.resize(tmp_img, (2034, 2048),
                         interpolation=cv2.INTER_NEAREST)
    tmp_img = NormalizeData(tmp_img) * 15
    tmp_img = tmp_img.astype('uint8') * 5
    rgb_img = np.zeros((tmp_img.shape[0], tmp_img.shape[1], 3)).astype('uint8')
    for i in range(rgb_img.shape[0]):
        for j in range(rgb_img.shape[1]):
            rgb_img[i, j] = intensity2rgb(tmp_img[i, j])
    return rgb_img


def create_alpha_image(rgb_img):
    alpha_img = np.zeros(
        (rgb_img.shape[0], rgb_img.shape[1], 4), dtype=np.uint8)
    for x in range(rgb_img.shape[0]):
        for y in range(rgb_img.shape[1]):
            rgb = tuple(rgb_img[x, y])
            if rgb == (0, 0, 0):
                alpha_img[x, y] = (0, 0, 0, 0)
            else:
                alpha_img[x, y] = np.concatenate(
                    (rgb_img[x, y], np.array([255])))

    return alpha_img


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    # The name of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The path and the file to upload
    # source_file_name = "local/path/to/file"

    # The name of the file in GCS bucket once uploaded
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.cache_control = 'no-store, max-age=0, no-transform'
    blob.upload_from_filename(source_file_name)


def prediction_n_time_frame(model_instance, X, lead_time=4):
    # assume X is prepocessed

    nwcst = []

    for _ in range(lead_time):
        # make prediction
        pred = model_instance.predict(X)
        # append prediction to holder
        nwcst.append(post_processing(pred))
        # append prediction to the input shifted on one step ahead
        X = np.concatenate([X[::, ::, ::, 1:], pred], axis=-1)

    return nwcst


class RequestDTO(BaseModel):
    filepaths: List[str]


@app.post("/predict/")
async def predict(req: RequestDTO):
    img_list = append_picture(req.filepaths)
    # img_list = img_list[np.newaxis, :, :]
    # img_list = np.moveaxis(img_list, 1, -1)
    # y_pred = model.predict(img_list)
    # outcome = post_processing(y_pred)
    # save file
    BUCKET_NAME = "datasci-kalampree"

    nowcasts = prediction_n_time_frame(model, img_list, lead_time=4)
    imgs_filenames = []
    for i in range(1, len(nowcasts)+1):
        img = create_alpha_image(nowcasts[i-1])
        img = Image.fromarray(img)
        file_local_path = f'./prediction/{str(int(req.filepaths[-1].split("/")[-1].split(".")[0]) + 300 * i)}.png'

        img.save(file_local_path)

        file_storage_path = f'predictRadarNJ/{str(int(req.filepaths[-1].split("/")[-1].split(".")[0]))}/{str(int(req.filepaths[-1].split("/")[-1].split(".")[0]) + 300 * i)}.png'

        upload_blob(BUCKET_NAME, file_local_path, file_storage_path)
        imgs_filenames.append(file_storage_path)
    # return StreamingResponse(BytesIO(outcome.tobytes()), media_type="image/png")
    return {"prediction": imgs_filenames}

nest_asyncio.apply()
uvicorn.run(app, port=8000)
