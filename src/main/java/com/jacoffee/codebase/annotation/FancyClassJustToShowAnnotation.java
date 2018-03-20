package com.jacoffee.codebase.annotation;

import java.lang.annotation.Annotation;

@AnnotationDemo(fancy=true, order=50)
public class FancyClassJustToShowAnnotation {

    public static void main(String[] args) {
        Class<?> c = FancyClassJustToShowAnnotation.class;
        System.out.println("Class " + c.getName() + " has these annotatioons: ");
        for (Annotation annotation: c.getAnnotations()) {
            if (annotation instanceof AnnotationDemo) {
                // pattern match in scala
                AnnotationDemo ad = (AnnotationDemo)annotation;
                System.out.println(ad.fancy());
                System.out.println(ad.order());
            }
        }
    }

}
