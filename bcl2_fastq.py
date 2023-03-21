import uuid
import subprocess
import os
import shlex
import run_qc_yaml_interop
from argparse import ArgumentParser

def download_folder(s3_path, dir_to_download):
    """
    Downloads the data from s3
    :param s3_path: s3 path
    :param dir_to_download: directory to download
    """
    cmd = 's5cmd --numworkers 10 cp --sse aws:kms --sse-kms-key-id arn:aws:kms:us-west-2:433453075219:key/f6993f44-4378-4b13-8926-a706061b17ec %s %s' % (s3_path, dir_to_download)
    subprocess.check_call(shlex.split(cmd))

def upload_folder(dir_to_upload):
    """
    Uploads the data to s3
    :param s3_path: s3 path
    :param dir_to_upload: directory to upload
    """
    dirs = [d for d in os.listdir(dir_to_upload) if os.path.isdir(os.path.join(dir_to_upload, d))]
    if any('-SR-' in d for d in dirs):
        cmd = 's5cmd --numworkers 10 sync --exclude "*-TW*"  --sse aws:kms --sse-kms-key-id arn:aws:kms:us-west-2:433453075219:key/f6993f44-4378-4b13-8926-a706061b17ec %s %s' % (dir_to_upload, 's3://ccgl-dnanexus-104-4-c-us-west-2.sec.ucsf.edu/')
        subprocess.check_call(shlex.split(cmd))
    if any('-TW' in d for d in dirs):
        cmd = 's5cmd --numworkers 10 sync --exclude "*-SR-*"  --sse aws:kms --sse-kms-key-id arn:aws:kms:us-west-2:433453075219:key/f6993f44-4378-4b13-8926-a706061b17ec %s %s' % (dir_to_upload, 's3://gml-exome-104-4-c-us-west-2.sec.ucsf.edu/')
        subprocess.check_call(shlex.split(cmd))

def generate_working_dir(working_dir_base):
    """
    Creates a unique working directory to combat job multitenancy
    :param working_dir_base: base working directory
    :return: a unique subfolder in working_dir_base with a uuid
    """
    working_dir = os.path.join(working_dir_base, str(uuid.uuid4()))
    #working_dir=os.path.join(working_dir_base,'ef3ee342-c5c0-41ea-8c15-372b89d0b892')
    try:
        os.mkdir(working_dir)
    except Exception as e:
        return working_dir_base
    return working_dir


def delete_working_dir(working_dir):
    """
    Deletes working directory
    :param working_dir:  working directory
    """

    try:
        shutil.rmtree(working_dir)
    except Exception as e:
        print ('Can\'t delete %s' % working_dir)

def download_bcl_files(bcl_s3_path, working_dir):
    """
    Downlodas the BCL files
    :param bcl_s3_path: S3 path containing BCLs
    :param working_dir: working directory
    :return: local path to the folder containing the fastq
    """
    bcl_folder = os.path.join(working_dir, 'bcls')

    try:
        os.mkdir(bcl_folder)
    except Exception as e:
        pass

    download_folder(bcl_s3_path, bcl_folder)

    return bcl_folder

def run_pipeline(bcls_folder_path, working_dir, samplesheet, flowcell_name):
    """
    Runs bcl2fastq
    :param ref_directory: local path to directory containing reference data
    :param fastq_folder_path: local path to directory contaning fastq files
    :param working_dir: working directory
    :param cmd_args: additional command line arguments to pass in
    :param is_vdj: runs vdj pipleine
    :return: path to results
    """

    os.chdir(working_dir)
    
    out_dir = os.path.join(working_dir, flowcell_name)
    input_dir = os.path.join(bcls_folder_path, 'Data', 'Intensities', 'BaseCalls')
    interop_dir = os.path.join(bcls_folder_path, 'InterOp')
    samplesheet = os.path.join(bcls_folder_path, samplesheet)
    cmd = 'bcl2fastq --runfolder-dir {0} --input-dir {1} --interop-dir {2} --barcode-mismatches {3} --output-dir {4} --sample-sheet {5}'.format(bcls_folder_path, input_dir, interop_dir, 0, out_dir, samplesheet)
	#cmd = 'cellranger count --id={0} --transcriptome={1} --fastqs={2} {3}'.format(run_id, ref_directory, fastq_folder_path, cmd_args)
	
    print("Running cmd {0}".format(cmd))
    subprocess.check_call(cmd, shell=True)

    run_qc_yaml_interop.execute(bcls_folder_path, out_dir)

    return out_dir
	

def main():
    argparser = ArgumentParser()
	
    file_path_group = argparser.add_argument_group(title='File paths')
    file_path_group.add_argument('--bcl_s3_path', type=str, help='BCL s3 path', required=True)
    #file_path_group.add_argument('--out_s3_path', type=str, help='s3 path to upload cellranger results', required=True)

    run_group = argparser.add_argument_group(title='Run command args')
    run_group.add_argument('--samplesheet', type=str, default='SampleSheet.csv')
    run_group.add_argument('--flowcell', type=str)

    argparser.add_argument('--working_dir', type=str, default='/scratch')

    args = argparser.parse_args()

    working_dir = generate_working_dir(args.working_dir)

    print("Downloading bcl files")
    bcls_folder_path = download_bcl_files(args.bcl_s3_path, working_dir)

    print("running bcl2fastq")
    out_files_path = run_pipeline(bcls_folder_path, working_dir, args.samplesheet, args.flowcell)

    print("Copying RunInfo")
    shutil.copy(os.path.join(bcls_folder_path, 'RunInfo.xml'), os.path.join(out_files_path,'RunInfo.xml'))

    print("uploading results")
    upload_folder(out_files_path)

    #print("cleaning up working direcotry")
    #delete_working_dir(working_dir)

    print("Completed")

if __name__ == '__main__':
    main()
