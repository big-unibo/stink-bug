from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib import rc

def savePdf(name, fig) :
    pp = PdfPages(name + '.pdf')
    pp.savefig( fig, dpi=300, bbox_inches = "tight" )
    pp.close()



rc('text', usetex=True)
def set_font_size(size) :
    rc_fonts = {
        "text.usetex": True,
        "font.size": size,
        'mathtext.default': 'regular',
        'axes.titlesize': size,
        "axes.labelsize": size,
        "legend.fontsize": size,
        "xtick.labelsize": size,
        "ytick.labelsize": size,
        'figure.titlesize': size,
        'text.latex.preamble': [r'\usepackage{amsmath,amssymb,bm,physics,lmodern}'],
        "font.family": "serif",
        "font.serif": "computer modern roman",
    }
    plt.rcParams.update(rc_fonts)