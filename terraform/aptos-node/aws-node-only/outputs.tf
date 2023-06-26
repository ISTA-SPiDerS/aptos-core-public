output "aws_eks_cluster" {
  value     = aws_eks_cluster.aptos
  sensitive = true
}

output "aws_eks_cluster_auth_token" {
  value     = data.aws_eks_cluster_auth.aptos.token
  sensitive = true
}

output "oidc_provider" {
  value     = local.oidc_provider
  sensitive = true
}


### Node outputs


### Network outputs

output "vpc_id" {
  value     = aws_vpc.vpc.id
  sensitive = true
}

output "aws_subnet_public" {
  value = aws_subnet.public
}

output "aws_subnet_private" {
  value = aws_subnet.private
}

output "aws_vpc_cidr_block" {
  value = aws_vpc.vpc.cidr_block
}

output "aws_eip_nat_public_ip" {
  value = aws_eip.nat.public_ip
}

output "cluster_security_group_id" {
  value = aws_eks_cluster.aptos.vpc_config[0].cluster_security_group_id
}
