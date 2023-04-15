import ai.djl.Application;
import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.output.DetectedObjects;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.translate.TranslateException;


import java.io.IOException;


import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;

import java.io.IOException;
import java.net.URL;

public final class Main {

    private Main() {}

    public static void main(String[] args) throws IOException, ModelException, TranslateException {
        Criteria<URL, Classifications> criteria =
                Criteria.builder()
                        .setTypes(URL.class, Classifications.class)
                        .optProgress(new ProgressBar())
                        .optEngine("PyTorch")
                        .optModelUrls("djl://ai.djl.pytorch/resnet/0.0.1/traced_resnet18")
                        .build();

        URL url = new URL("https://resources.djl.ai/images/kitten.jpg");
        try (ZooModel<URL, Classifications> model = criteria.loadModel();
             Predictor<URL, Classifications> predictor = model.newPredictor()) {
            Classifications classifications = predictor.predict(url);
            System.out.println(classifications);
        }
    }
}
