import os
import tempfile
import aspose.pdf as ap

class ConversionError(Exception):
    pass

def convert_pdf_bytes_to_xlsx_bytes(pdf_bytes, minimize_worksheets=True):
    if not pdf_bytes:
        raise ValueError("pdf_bytes vazio.")

    in_tmp = None
    out_tmp = None

    try:
        # Cria um PDF temporário (entrada)
        f_in = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        in_tmp = f_in.name
        f_in.write(pdf_bytes)
        f_in.close()

        # Cria um XLSX temporário (saída)
        f_out = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        out_tmp = f_out.name
        f_out.close()

        # Abre e converte
        doc = ap.Document(in_tmp)
        opts = ap.ExcelSaveOptions()
        opts.format = ap.ExcelSaveOptions.ExcelFormat.XLSX
        opts.minimize_the_number_of_worksheets = bool(minimize_worksheets)

        doc.save(out_tmp, opts)

        # Lê bytes do XLSX gerado
        with open(out_tmp, "rb") as f:
            xlsx_bytes = f.read()
        if not xlsx_bytes:
            raise ConversionError("Conversão gerou XLSX vazio.")

        return xlsx_bytes

    except Exception as e:
        raise ConversionError("Falha na conversão: %s" % e)

    finally:
        for path in (in_tmp, out_tmp):
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except:
                pass

def sniff_output_filename(input_filename):
    base = os.path.basename(input_filename)
    if base.lower().endswith(".pdf"):
        base = base[:-4]

    return base + ".xlsx"

